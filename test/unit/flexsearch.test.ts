import assert from 'assert';
import AsyncTestUtil from 'async-test-util';
import config, { describeParallel } from './config.ts';

import {
    addRxPlugin,
    clone,
    createBlob,
    createRevision,
    createRxDatabase,
    randomToken,
    RxCollection,
    RxJsonSchema
} from '../../plugins/core/index.mjs';
import { RxDBAttachmentsPlugin } from '../../plugins/attachments/index.mjs';
import {
    getFlexSearchState,
    RxDBFlexSearchPlugin,
    wrappedFlexSearchStorage,
    type FlexSearchMetaDocumentData,
    type FlexSearchSearchOptions
} from '../../plugins/flexsearch/index.mjs';
import {
    schemaObjects,
    schemas
} from '../../plugins/test-utils/index.mjs';
import type { HumanDocumentType } from '../../src/plugins/test-utils/schemas.ts';

addRxPlugin(RxDBFlexSearchPlugin);
addRxPlugin(RxDBAttachmentsPlugin);

type Collections = {
    humans: RxCollection<HumanDocumentType>;
};

type FlexSearchCollection = RxCollection<HumanDocumentType> & {
    fts(
        queryOrOptions?: string | FlexSearchSearchOptions,
        selector?: Record<string, unknown>
    ): {
        exec(): Promise<HumanDocumentType[]>;
    };
};

const FTS_META_DOC_ID = 'index-state';

function getMetaStorage(db: Awaited<ReturnType<typeof createDatabase>>['db']) {
    const metaStorage = getFlexSearchState(db.name, 'humans')?.metaStorage;
    if (!metaStorage) {
        throw new Error('Expected flexsearch meta storage to exist');
    }
    return metaStorage;
}

function getSortedIds(docs: HumanDocumentType[]): string[] {
    return docs.map((doc: HumanDocumentType) => doc.passportId).sort();
}

function getSchemaWithFts(
    fields: Array<'firstName' | 'lastName'>,
    withAttachments = false,
    fieldOverrides: Partial<Record<'firstName' | 'lastName', Record<string, unknown>>> = {}
): RxJsonSchema<HumanDocumentType> {
    const schema = clone(schemas.human) as RxJsonSchema<HumanDocumentType>;
    if (withAttachments) {
        schema.attachments = {};
    }
    fields.forEach(fieldName => {
        const fieldSchema = clone((schema.properties as Record<string, unknown>)[fieldName]) as Record<string, unknown>;
        fieldSchema.fts = {
            tokenize: 'forward',
            resolution: 9,
            ...(fieldOverrides[fieldName] ?? {})
        };
        (schema.properties as Record<string, unknown>)[fieldName] = fieldSchema;
    });
    return schema;
}

async function createDatabase(
    input: {
        name?: string;
        schema?: RxJsonSchema<HumanDocumentType>;
        persistence?: {
            minDebounce?: number;
            maxDebounce?: number;
            adaptive?: boolean;
        };
        defaultIndexOptions?: Partial<FlexSearchSearchOptions>;
        multiInstance?: boolean;
    } = {}
) {
    const db = await createRxDatabase<Collections>({
        name: input.name ?? randomToken(10),
        storage: wrappedFlexSearchStorage({
            storage: config.storage.getStorage(),
            persistence: input.persistence,
            defaultIndexOptions: input.defaultIndexOptions as never
        }),
        multiInstance: input.multiInstance ?? false
    });
    const collections = await db.addCollections({
        humans: {
            schema: input.schema ?? getSchemaWithFts(['firstName'])
        }
    });
    return {
        db,
        collection: collections.humans
    };
}

async function waitForPersistence(time = 260) {
    await AsyncTestUtil.wait(time);
}

async function readMetaState(db: Awaited<ReturnType<typeof createDatabase>>['db']) {
    const metaStorage = getMetaStorage(db);
    const docs = await metaStorage.findDocumentsById([FTS_META_DOC_ID], true);
    return docs[0];
}

describeParallel('flexsearch.test.ts', () => {
    it('passes through collections without fts fields', async () => {
        const db = await createRxDatabase<Collections>({
            name: randomToken(10),
            storage: wrappedFlexSearchStorage({
                storage: config.storage.getStorage()
            }),
            multiInstance: false
        });
        const collections = await db.addCollections({
            humans: {
                schema: schemas.human
            }
        });

        await collections.humans.insert(schemaObjects.humanData());
        const docs = await collections.humans.find().exec();
        assert.strictEqual(docs.length, 1);
        assert.strictEqual(getFlexSearchState(db.name, 'humans'), undefined);

        await db.remove();
    });

    it('indexes and queries with $fts plus additional RxDB filters', async () => {
        const { db, collection } = await createDatabase({
            schema: getSchemaWithFts(['firstName', 'lastName'])
        });

        await collection.bulkInsert([
            { passportId: 'a1', firstName: 'Alice', lastName: 'Anderson', age: 31 },
            { passportId: 'a2', firstName: 'Alice', lastName: 'Baker', age: 24 },
            { passportId: 'b1', firstName: 'Bob', lastName: 'Builder', age: 44 }
        ]);

        // FlexSearch indexing is asynchronous — the index is updated via a changeStream
        // subscription that fires after bulkWrite completes, not inline. waitUntil polls
        // until the FTS index reflects the writes, acting as a synchronisation barrier
        // before the real assertion. There is no error to catch; the query succeeds
        // immediately but returns 0 results until the index catches up.
        await AsyncTestUtil.waitUntil(async () => {
            const docs = await collection.find({
                selector: {
                    $fts: 'Ali' as never,
                    age: { $gt: 30 }
                }
            }).exec();
            return docs.length === 1;
        });

        const results = await collection.find({
            selector: {
                $fts: 'Ali' as never,
                age: { $gt: 30 }
            }
        }).exec();

        assert.strictEqual(results.length, 1);
        assert.strictEqual(results[0].passportId, 'a1');

        await db.remove();
    });

    it('keeps full RxDB document data available after $fts filtering, including attachments', async function () {
        if (!config.storage.hasAttachments) {
            this.skip();
        }

        const { db, collection } = await createDatabase({
            schema: getSchemaWithFts(['firstName'], true)
        });

        const doc = await collection.insert({
            passportId: 'att-1',
            firstName: 'Mara',
            lastName: 'Miller',
            age: 20
        });
        await doc.putAttachment({
            id: 'hello.txt',
            data: createBlob('hello flexsearch', 'text/plain'),
            type: 'text/plain'
        });

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await collection.find({
                selector: {
                    $fts: 'Mar' as never
                }
            }).exec();
            return docs.length === 1;
        });

        const results = await collection.find({
            selector: {
                $fts: 'Mar' as never
            }
        }).exec();
        assert.strictEqual(results.length, 1);
        assert.strictEqual(results[0].passportId, 'att-1');
        const attachment = results[0].getAttachment('hello.txt');
        assert.ok(attachment);
        assert.strictEqual(await attachment.getStringData(), 'hello flexsearch');

        await db.remove();
    });

    it('updates index only after successful write operations via changeStream', async () => {
        const { db, collection } = await createDatabase();
        const inserted = await collection.insert({
            passportId: 'up-1',
            firstName: 'Alice',
            lastName: 'Anderson',
            age: 20
        });

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await collection.find({
                selector: {
                    $fts: 'Ali' as never
                }
            }).exec();
            return docs.length === 1;
        });

        let results = await collection.find({
            selector: {
                $fts: 'Ali' as never
            }
        }).exec();
        assert.strictEqual(results.length, 1);

        const updated = await inserted.patch({ firstName: 'Alicia' });
        await AsyncTestUtil.waitUntil(async () => {
            const docs = await collection.find({
                selector: {
                    $fts: 'Alic' as never
                }
            }).exec();
            return docs.length === 1;
        });

        results = await collection.find({
            selector: {
                $fts: 'Alice' as never
            }
        }).exec();
        assert.strictEqual(results.length, 0);

        await updated.remove();
        await AsyncTestUtil.waitUntil(async () => {
            const docs = await collection.find({
                selector: {
                    $fts: 'Alic' as never
                }
            }).exec();
            return docs.length === 0;
        });

        await db.remove();
    });

    it('provides collection.fts() helper as sugar over find()', async () => {
        const { db, collection } = await createDatabase();

        await collection.insert({
            passportId: 'h1',
            firstName: 'Mara',
            lastName: 'Miller',
            age: 20
        });

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await (collection as FlexSearchCollection).fts('Mar').exec();
            return docs.length === 1;
        });

        const helperResults = await (collection as FlexSearchCollection).fts('Mar').exec();
        assert.strictEqual(helperResults.length, 1);
        assert.strictEqual(helperResults[0].passportId, 'h1');

        await db.remove();
    });

    it('isolates indexes across multiple fts-enabled collections', async () => {
        const db = await createRxDatabase<{
            heroes: RxCollection<HumanDocumentType>;
            villains: RxCollection<HumanDocumentType>;
        }>({
            name: randomToken(10),
            storage: wrappedFlexSearchStorage({
                storage: config.storage.getStorage(),
                persistence: {
                    minDebounce: 20,
                    maxDebounce: 120
                }
            }),
            multiInstance: false
        });

        const collections = await db.addCollections({
            heroes: {
                schema: getSchemaWithFts(['firstName'])
            },
            villains: {
                schema: getSchemaWithFts(['lastName'])
            }
        });

        await collections.heroes.insert({
            passportId: 'hero-1',
            firstName: 'Clark',
            lastName: 'Kent',
            age: 30
        });
        await collections.villains.insert({
            passportId: 'villain-1',
            firstName: 'Lex',
            lastName: 'Luthor',
            age: 45
        });

        await AsyncTestUtil.waitUntil(async () => {
            const heroDocs = await collections.heroes.find({ selector: { $fts: 'Cla' as never } }).exec();
            const villainDocs = await collections.villains.find({ selector: { $fts: 'Luth' as never } }).exec();
            return heroDocs.length === 1 && villainDocs.length === 1;
        });

        const crossResults = await collections.heroes.find({
            selector: {
                $fts: 'Luth' as never
            }
        }).exec();
        assert.strictEqual(crossResults.length, 0);

        await db.remove();
    });

    it('persists serialized snapshot and restores after reopen', async () => {
        const name = randomToken(10);

        const first = await createDatabase({
            name,
            persistence: {
                minDebounce: 20,
                maxDebounce: 120
            }
        });
        await first.collection.insert({
            passportId: 'p1',
            firstName: 'Oliver',
            lastName: 'Queen',
            age: 40
        });

        await waitForPersistence();
        const persistedMeta = await readMetaState(first.db);
        assert.ok(persistedMeta);
        assert.ok(Array.isArray(persistedMeta.serialized));
        assert.ok((persistedMeta.serialized?.length ?? 0) > 0);
        assert.strictEqual(persistedMeta.checkpointId, 'p1');

        await first.db.close();

        const second = await createDatabase({
            name,
            persistence: {
                minDebounce: 20,
                maxDebounce: 120
            }
        });

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await second.collection.find({
                selector: {
                    $fts: 'Oliv' as never
                }
            }).exec();
            return docs.length === 1;
        });

        const restored = await second.collection.find({
            selector: {
                $fts: 'Oliv' as never
            }
        }).exec();
        assert.strictEqual(restored.length, 1);
        assert.strictEqual(restored[0].passportId, 'p1');

        await second.db.remove();
    });

    it('catches up writes that happened after the last persisted checkpoint', async () => {
        const name = randomToken(10);

        const first = await createDatabase({
            name,
            persistence: {
                minDebounce: 20,
                maxDebounce: 120
            }
        });
        await first.collection.insert({
            passportId: 'c1',
            firstName: 'Oliver',
            lastName: 'Queen',
            age: 40
        });
        await waitForPersistence();

        await first.collection.insert({
            passportId: 'c2',
            firstName: 'Hal',
            lastName: 'Jordan',
            age: 37
        });
        await first.db.close();

        const second = await createDatabase({
            name,
            persistence: {
                minDebounce: 20,
                maxDebounce: 120
            }
        });

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await second.collection.find({
                selector: {
                    $fts: 'Hal' as never
                }
            }).exec();
            return docs.length === 1;
        });

        const catchup = await second.collection.find({
            selector: {
                $fts: 'Hal' as never
            }
        }).exec();
        assert.strictEqual(catchup.length, 1);
        assert.strictEqual(catchup[0].passportId, 'c2');

        await second.db.remove();
    });

    it('falls back to rebuild when the persisted snapshot is corrupted', async () => {
        const name = randomToken(10);

        const first = await createDatabase({
            name,
            persistence: {
                minDebounce: 20,
                maxDebounce: 120
            }
        });
        await first.collection.insert({
            passportId: 'r1',
            firstName: 'Barry',
            lastName: 'Allen',
            age: 29
        });
        await waitForPersistence();

        const metaStorage = getMetaStorage(first.db);
        const previous = (await metaStorage.findDocumentsById([FTS_META_DOC_ID], true))[0];
        assert.ok(previous);
        const corrupted = clone(previous) as FlexSearchMetaDocumentData;
        corrupted.serialized = [1, 2, 3, 4, 5];
        corrupted._meta.lwt = previous._meta.lwt + 1;
        corrupted._rev = createRevision(first.db.token, previous);
        const corruptedWrite = await metaStorage.bulkWrite([
            {
                previous,
                document: corrupted
            }
        ], 'corrupt-flexsearch-snapshot');
        assert.strictEqual(corruptedWrite.error.length, 0);

        await first.db.close();

        const second = await createDatabase({
            name,
            persistence: {
                minDebounce: 20,
                maxDebounce: 120
            }
        });

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await second.collection.find({
                selector: {
                    $fts: 'Barr' as never
                }
            }).exec();
            return docs.length === 1;
        });

        const rebuilt = await second.collection.find({
            selector: {
                $fts: 'Barr' as never
            }
        }).exec();
        assert.strictEqual(rebuilt.length, 1);
        const repairedMeta = await readMetaState(second.db);
        assert.ok(repairedMeta);
        const repairedMetaTyped = repairedMeta as FlexSearchMetaDocumentData;
        assert.ok(Array.isArray(repairedMetaTyped.serialized));
        assert.ok((repairedMetaTyped.serialized?.length ?? 0) > 0);
        assert.notStrictEqual(repairedMetaTyped.serialized, corrupted.serialized);

        await second.db.remove();
    });

    it('fts() accepts a limit option to cap results', async () => {
        const { db, collection } = await createDatabase();

        await collection.bulkInsert([
            { passportId: 'lim-1', firstName: 'Alice', lastName: 'One', age: 21 },
            { passportId: 'lim-2', firstName: 'Alice', lastName: 'Two', age: 22 },
            { passportId: 'lim-3', firstName: 'Alice', lastName: 'Three', age: 23 }
        ]);

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await (collection as FlexSearchCollection).fts('Alice').exec();
            return docs.length === 3;
        });

        const limitedResults = await (collection as FlexSearchCollection).fts({ query: 'Alice', limit: 1 }).exec();
        assert.strictEqual(limitedResults.length, 1);

        await db.remove();
    });

    it('fts() accepts a field option to restrict search to one field', async () => {
        const { db, collection } = await createDatabase({
            schema: getSchemaWithFts(['firstName', 'lastName'])
        });

        await collection.bulkInsert([
            { passportId: 'fld-1', firstName: 'Carol', lastName: 'Alpha', age: 30 },
            { passportId: 'fld-2', firstName: 'Alpha', lastName: 'Bravo', age: 31 }
        ]);

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await (collection as FlexSearchCollection).fts('Alpha').exec();
            return docs.length === 2;
        });

        // Restrict to firstName only - only fld-2 has "Alpha" as firstName
        const firstNameOnly = await (collection as FlexSearchCollection)
            .fts({ query: 'Alpha', field: 'firstName' }).exec();
        assert.strictEqual(firstNameOnly.length, 1);
        assert.strictEqual(firstNameOnly[0].passportId, 'fld-2');

        await db.remove();
    });

    it('fts() suggest:true returns results when only some query tokens match', async () => {
        // suggest:true is a QUERY-TIME option for multi-word queries: when some query tokens
        // have no index matches, suggest:true still returns results for the matched tokens;
        // suggest:false (default) requires ALL tokens to match.
        // This is entirely separate from tokenize:'forward' prefix matching — searching
        // 'Alexan' to find 'Alexandra' works via tokenization at write time, not suggest.
        // See the next test ('tokenize:forward') for character-level prefix matching.
        const { db, collection } = await createDatabase({
            schema: getSchemaWithFts(['firstName'])
        });

        await collection.insert({
            passportId: 'sug-1',
            firstName: 'Alexandra',
            lastName: 'Smith',
            age: 25
        });

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await (collection as FlexSearchCollection).fts('Alexandra').exec();
            return docs.length === 1;
        });

        // Multi-word query where 'Zzzznotreal' matches nothing:
        // suggest:false requires ALL tokens to match → 0 results
        const noSuggest = await (collection as FlexSearchCollection)
            .fts({ query: 'Alexandra Zzzznotreal', suggest: false }).exec();
        assert.strictEqual(noSuggest.length, 0);

        // suggest:true only requires at least one token to match → returns the Alexandra doc
        const withSuggest = await (collection as FlexSearchCollection)
            .fts({ query: 'Alexandra Zzzznotreal', suggest: true }).exec();
        assert.strictEqual(withSuggest.length, 1);
        assert.strictEqual(withSuggest[0].passportId, 'sug-1');

        await db.remove();
    });

    it('tokenize:forward indexes every prefix enabling partial terms to find full words', async () => {
        // tokenize:'forward' (the default in all tests via getSchemaWithFts) indexes every
        // leading prefix of each token at write time. This is what makes 'Alexan' find
        // 'Alexandra' — no suggest is involved. tokenize:'reverse' would index suffixes;
        // tokenize:'full' would index every substring; tokenize:'strict' only exact words.
        const { db, collection } = await createDatabase();

        await collection.insert({
            passportId: 'pfx-1',
            firstName: 'Alexandra',
            lastName: 'Smith',
            age: 25
        });

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await (collection as FlexSearchCollection).fts('Alexandra').exec();
            return docs.length === 1;
        });

        // Any leading prefix matches
        const byAlexan = await (collection as FlexSearchCollection).fts('Alexan').exec();
        assert.strictEqual(byAlexan.length, 1);
        assert.strictEqual(byAlexan[0].passportId, 'pfx-1');

        const byAlex = await (collection as FlexSearchCollection).fts('Alex').exec();
        assert.strictEqual(byAlex.length, 1);
        assert.strictEqual(byAlex[0].passportId, 'pfx-1');

        // An internal substring that is NOT a leading prefix does not match
        const byNonPrefix = await (collection as FlexSearchCollection).fts('lexand').exec();
        assert.strictEqual(byNonPrefix.length, 0);

        await db.remove();
    });

    it('$fts selector accepts full search options object with query, limit, suggest', async () => {
        const { db, collection } = await createDatabase();

        await collection.bulkInsert([
            { passportId: 'opt-1', firstName: 'Benjamin', lastName: 'Carter', age: 28 },
            { passportId: 'opt-2', firstName: 'Benito', lastName: 'Cruz', age: 29 },
            { passportId: 'opt-3', firstName: 'Ben', lastName: 'Davis', age: 30 }
        ]);

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await collection.find({
                selector: { $fts: 'Ben' as never }
            }).exec();
            return docs.length === 3;
        });

        // Pass options object: limit to 1 result
        const limitedResults = await collection.find({
            selector: { $fts: { query: 'Ben', limit: 1 } as never }
        }).exec();
        assert.strictEqual(limitedResults.length, 1);

        await db.remove();
    });

    it('field resolution controls intra-field score granularity; Document.search returns per-field rows in definition order', async () => {
        // resolution determines how many scoring buckets a field has. Higher resolution yields
        // finer relevance ranking WITHIN that field (more buckets = better ability to distinguish
        // a top-of-field match from a buried one). It is NOT what determines the ORDER of field
        // rows in the Document.search() result array — that order follows the field definition
        // order (or explicit `priority`). We verify both the raw per-field ordering AND that
        // fts() surfaces both matched docs.
        const { db, collection } = await createDatabase({
            schema: getSchemaWithFts(['firstName', 'lastName'], false, {
                firstName: { tokenize: 'forward', resolution: 9 },
                lastName: { tokenize: 'forward', resolution: 3 }
            })
        });

        await collection.bulkInsert([
            { passportId: 'res-1', firstName: 'Target', lastName: 'Other', age: 20 },
            { passportId: 'res-2', firstName: 'Other', lastName: 'Target', age: 21 }
        ]);

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await (collection as FlexSearchCollection).fts('Target').exec();
            return docs.length === 2;
        });

        // Raw FlexSearch returns one row per matched field, in field definition order.
        // firstName (resolution:9, defined first) → row 0, containing res-1.
        // lastName  (resolution:3, defined second) → row 1, containing res-2.
        const state = getFlexSearchState(db.name, 'humans');
        assert.ok(state);
        type FieldRow = { field?: string; result: unknown[] };
        const rawResult = state.index.search('Target' as any) as FieldRow[];
        assert.strictEqual(rawResult.length, 2);
        assert.strictEqual(rawResult[0].field, 'firstName');
        assert.deepStrictEqual(
            [...rawResult[0].result].map(v => String(v)).sort(),
            ['res-1']
        );
        assert.strictEqual(rawResult[1].field, 'lastName');
        assert.deepStrictEqual(
            [...rawResult[1].result].map(v => String(v)).sort(),
            ['res-2']
        );

        // fts() integrates both field rows and returns all matched docs
        const allResults = await (collection as FlexSearchCollection).fts('Target').exec();
        assert.deepStrictEqual(getSortedIds(allResults), ['res-1', 'res-2']);

        // Restricting to a single field returns only the doc matched via that field
        const firstNameMatch = await (collection as FlexSearchCollection)
            .fts({ query: 'Target', field: 'firstName' }).exec();
        assert.strictEqual(firstNameMatch.length, 1);
        assert.strictEqual(firstNameMatch[0].passportId, 'res-1');

        const lastNameMatch = await (collection as FlexSearchCollection)
            .fts({ query: 'Target', field: 'lastName' }).exec();
        assert.strictEqual(lastNameMatch.length, 1);
        assert.strictEqual(lastNameMatch[0].passportId, 'res-2');

        await db.remove();
    });

    it('supports query variations for fts() and $fts (string, $eq, options.query)', async () => {
        const { db, collection } = await createDatabase({
            schema: getSchemaWithFts(['firstName', 'lastName'])
        });

        await collection.bulkInsert([
            { passportId: 'var-1', firstName: 'Martha', lastName: 'Jones', age: 32 },
            { passportId: 'var-2', firstName: 'Margo', lastName: 'Lane', age: 33 },
            { passportId: 'var-3', firstName: 'Lena', lastName: 'Martha', age: 34 }
        ]);

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await (collection as FlexSearchCollection).fts('Marth').exec();
            return docs.length === 2;
        });

        const viaFtsString = await (collection as FlexSearchCollection).fts('Marth').exec();
        const viaFtsOptions = await (collection as FlexSearchCollection).fts({ query: 'Marth' }).exec();
        const viaSelectorString = await collection.find({ selector: { $fts: 'Marth' as never } }).exec();
        const viaSelectorEq = await collection.find({ selector: { $fts: { $eq: 'Marth' } as never } }).exec();
        const viaSelectorOptions = await collection.find({ selector: { $fts: { query: 'Marth' } as never } }).exec();

        assert.deepStrictEqual(getSortedIds(viaFtsString), ['var-1', 'var-3']);
        assert.deepStrictEqual(getSortedIds(viaFtsOptions), ['var-1', 'var-3']);
        assert.deepStrictEqual(getSortedIds(viaSelectorString), ['var-1', 'var-3']);
        assert.deepStrictEqual(getSortedIds(viaSelectorEq), ['var-1', 'var-3']);
        assert.deepStrictEqual(getSortedIds(viaSelectorOptions), ['var-1', 'var-3']);

        await db.remove();
    });

    it('limit and offset FlexSearch options are applied at the search level', async () => {
        const { db, collection } = await createDatabase();

        await collection.bulkInsert([
            { passportId: 'off-1', firstName: 'Alice', lastName: 'One', age: 21 },
            { passportId: 'off-2', firstName: 'Alice', lastName: 'Two', age: 22 },
            { passportId: 'off-3', firstName: 'Alice', lastName: 'Three', age: 23 }
        ]);

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await collection.find({ selector: { $fts: 'Alice' as never } }).exec();
            return docs.length === 3;
        });

        // limit:2 caps the number of FlexSearch result IDs passed to $in → exactly 2 docs back
        const limitedTwo = await collection.find({
            selector: { $fts: { query: 'Alice', limit: 2 } as never }
        }).exec();
        assert.strictEqual(limitedTwo.length, 2);
        assert.ok(limitedTwo.every(d => d.firstName === 'Alice'));

        // offset:1 skips the first result; 3 total − 1 skipped = 2 remaining (within limit:10)
        const afterFirst = await collection.find({
            selector: { $fts: { query: 'Alice', offset: 1, limit: 10 } as never }
        }).exec();
        assert.strictEqual(afterFirst.length, 2);

        // Two pages (0..1, 2..3) together must cover all 3 unique docs
        const page1 = await collection.find({
            selector: { $fts: { query: 'Alice', offset: 0, limit: 2 } as never }
        }).exec();
        const page2 = await collection.find({
            selector: { $fts: { query: 'Alice', offset: 2, limit: 2 } as never }
        }).exec();
        const allPaged = [...new Set([...getSortedIds(page1), ...getSortedIds(page2)].sort())];
        assert.deepStrictEqual(allPaged, ['off-1', 'off-2', 'off-3']);

        await db.remove();
    });

    it('passes through advanced document options with exact result behavior', async () => {
        const { db, collection } = await createDatabase({
            schema: getSchemaWithFts(['firstName', 'lastName'])
        });

        await collection.bulkInsert([
            { passportId: 'optx-1', firstName: 'Alexandra', lastName: 'Stone', age: 20 },
            { passportId: 'optx-2', firstName: 'Alex', lastName: 'Mason', age: 21 },
            { passportId: 'optx-3', firstName: 'Taylor', lastName: 'Alexandra', age: 22 }
        ]);

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await collection.find({ selector: { $fts: 'Alex' as never } }).exec();
            return docs.length === 3;
        });

        const state = getFlexSearchState(db.name, 'humans');
        assert.ok(state);

        const plainDocs = await collection.find({
            selector: { $fts: 'Alex' as never }
        }).exec();
        assert.deepStrictEqual(getSortedIds(plainDocs), ['optx-1', 'optx-2', 'optx-3']);

        const suggestDocs = await collection.find({
            selector: { $fts: { query: 'Alex Zzzz', suggest: true } as never }
        }).exec();
        assert.deepStrictEqual(getSortedIds(suggestDocs), ['optx-1', 'optx-2', 'optx-3']);

        const rawSuggestResults = state.index.search({ query: 'Alex Zzzz', suggest: true } as never);
        assert.ok(Array.isArray(rawSuggestResults));
        assert.ok(rawSuggestResults.length > 0);

        const pluckDocs = await collection.find({
            selector: { $fts: { query: 'Alex', pluck: 'firstName' } as never }
        }).exec();
        assert.deepStrictEqual(getSortedIds(pluckDocs), ['optx-1', 'optx-2']);

        const rawPluckResults = state.index.search({ query: 'Alex', pluck: 'firstName' } as never);
        assert.ok(Array.isArray(rawPluckResults));
        assert.deepStrictEqual(
            (rawPluckResults as unknown[]).map(value => String(value)).sort(),
            ['optx-1', 'optx-2']
        );

        const mergeDocs = await collection.find({
            selector: { $fts: { query: 'Alex', merge: true } as never }
        }).exec();
        assert.deepStrictEqual(getSortedIds(mergeDocs), ['optx-1', 'optx-2', 'optx-3']);

        const rawMergeResults = state.index.search({ query: 'Alex', merge: true } as never);
        assert.ok(Array.isArray(rawMergeResults));
        const mergedIds: string[] = [];
        for (const item of rawMergeResults as unknown[]) {
            if (item && typeof item === 'object' && 'id' in item) {
                const itemRecord = item as Record<string, unknown>;
                if (typeof itemRecord.id === 'string' || typeof itemRecord.id === 'number') {
                    mergedIds.push(String(itemRecord.id));
                }
            }
        }
        mergedIds.sort();
        assert.deepStrictEqual(mergedIds, ['optx-1', 'optx-2', 'optx-3']);

        const enrichDocs = await collection.find({
            selector: { $fts: { query: 'Alex', enrich: true } as never }
        }).exec();
        assert.deepStrictEqual(getSortedIds(enrichDocs), ['optx-1', 'optx-2', 'optx-3']);

        const rawEnrichResults = state.index.search({ query: 'Alex', enrich: true } as never);
        assert.ok(Array.isArray(rawEnrichResults));
        assert.ok(rawEnrichResults.length > 0);
        // enrich: true uses the same per-field row structure as a normal Document search:
        // [{ field, result: [id | {id, doc}, ...] }, ...] — one row per indexed field
        const firstEnrichRow = rawEnrichResults[0];
        assert.ok(firstEnrichRow && typeof firstEnrichRow === 'object' && 'result' in firstEnrichRow);
        // Collect IDs from ALL field rows (firstName row has optx-1/2, lastName row has optx-3)
        const enrichIds: string[] = [];
        for (const row of rawEnrichResults as unknown[]) {
            if (!row || typeof row !== 'object') continue;
            const rowRecord = row as Record<string, unknown>;
            if (!Array.isArray(rowRecord.result)) continue;
            for (const item of rowRecord.result as unknown[]) {
                if (typeof item === 'string' || typeof item === 'number') {
                    enrichIds.push(String(item));
                } else if (item && typeof item === 'object' && 'id' in item) {
                    const itemRecord = item as Record<string, unknown>;
                    if (typeof itemRecord.id === 'string' || typeof itemRecord.id === 'number') {
                        enrichIds.push(String(itemRecord.id));
                    }
                }
            }
        }
        assert.deepStrictEqual([...new Set(enrichIds)].sort(), ['optx-1', 'optx-2', 'optx-3']);

        await db.remove();
    });

    it('score function is invoked at indexing time and steers result ranking within a field', async () => {
        // score is an IndexOptions function: (content, term, termIndex, partial, partialIndex) => number
        // It is called when a document is added to the index. Return 0 (= highest-priority bucket)
        // up to `resolution` (lowest-priority bucket). This test verifies:
        //   1. The score function is actually called during indexing (side-effect counter).
        //   2. Docs indexed with score => 0 (highest bucket) appear in the raw per-field result.
        //
        // NOTE: The score function cannot be placed inside the schema's `fts` property because
        // RxDB serializes the schema as a metadata document — functions are not serializable.
        // Instead, pass score (and other function-valued options) via `defaultIndexOptions` in
        // wrappedFlexSearchStorage, which is never stored to disk.
        let scoreCallCount = 0;
        const customScore = (_content: string[], _term: string) => {
            scoreCallCount++;
            return 0; // always force into the top-priority (bucket 0) slot
        };

        const { db, collection } = await createDatabase({
            schema: getSchemaWithFts(['firstName'], false, {
                firstName: { tokenize: 'strict', resolution: 9 }
            }),
            defaultIndexOptions: { score: customScore as never }
        });

        await collection.bulkInsert([
            { passportId: 'sc-1', firstName: 'Zephyr', lastName: 'Alpha', age: 20 },
            { passportId: 'sc-2', firstName: 'Zephyr', lastName: 'Beta', age: 21 }
        ]);

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await (collection as FlexSearchCollection).fts('Zephyr').exec();
            return docs.length === 2;
        });

        // The score function must have been called at least once per document per term
        assert.ok(scoreCallCount > 0, `score function was not called; callCount=${scoreCallCount}`);

        // Verify fts() surfaces both docs
        const allDocs = await (collection as FlexSearchCollection).fts('Zephyr').exec();
        assert.deepStrictEqual(getSortedIds(allDocs), ['sc-1', 'sc-2']);

        // Raw FlexSearch result: with score always returning 0, both docs sit in bucket 0.
        const state = getFlexSearchState(db.name, 'humans');
        assert.ok(state);
        type FieldRow = { field?: string; result: unknown[] };
        const rawResult = state.index.search('Zephyr' as any) as FieldRow[];
        assert.ok(rawResult.length > 0, 'expected at least one field row');
        const allRawIds = rawResult.flatMap(row => row.result.map((id: unknown) => String(id))).sort();
        assert.deepStrictEqual(allRawIds, ['sc-1', 'sc-2']);

        await db.remove();
    });

    it('boost search option shifts result ranking in multi-field Document.search()', async () => {
        // boost is a Resolver / intersect option accepted at search time.
        // When searching a Document with multiple fields, FlexSearch merges the per-field
        // result lists via intersect/union. A boost value on the search options shifts the
        // score bucket of matching results, effectively raising their rank.
        //
        // This test verifies:
        //   1. Passing boost does not break ID extraction (same docs returned).
        //   2. A per-field descriptor array with boost on one field restricts results to
        //      only the documents matched in that field.
        const { db, collection } = await createDatabase({
            schema: getSchemaWithFts(['firstName', 'lastName'])
        });

        await collection.bulkInsert([
            { passportId: 'bo-1', firstName: 'Nimbus', lastName: 'Other', age: 20 },
            { passportId: 'bo-2', firstName: 'Other', lastName: 'Nimbus', age: 21 }
        ]);

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await (collection as FlexSearchCollection).fts('Nimbus').exec();
            return docs.length === 2;
        });

        // Plain search (no boost) returns both docs
        const plain = await (collection as FlexSearchCollection).fts('Nimbus').exec();
        assert.deepStrictEqual(getSortedIds(plain), ['bo-1', 'bo-2']);

        // Adding boost:5 at the top level must still return both docs without throwing
        const boosted = await (collection as FlexSearchCollection)
            .fts({ query: 'Nimbus', boost: 5 } as FlexSearchSearchOptions).exec();
        assert.deepStrictEqual(getSortedIds(boosted), ['bo-1', 'bo-2']);

        // Per-field boost array: restrict to firstName field with boost:10 — only bo-1 matches
        const state = getFlexSearchState(db.name, 'humans');
        assert.ok(state);
        // Directly exercise the FlexSearch Document API with a field-descriptor array
        const rawFieldBoosted = state.index.search({
            field: [{ field: 'firstName', query: 'Nimbus', boost: 10 }]
        } as any) as Array<{ field: string; result: unknown[] }>;
        assert.ok(Array.isArray(rawFieldBoosted), 'expected array result');
        assert.ok(rawFieldBoosted.length > 0, 'expected at least one field row');
        const fieldBoostedIds = rawFieldBoosted.flatMap(row => row.result.map((id: unknown) => String(id))).sort();
        assert.deepStrictEqual(fieldBoostedIds, ['bo-1']);

        await db.remove();
    });
});
