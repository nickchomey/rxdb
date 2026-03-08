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
    type FlexSearchMetaDocumentData
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
    fts(searchTerm: string, selector?: Record<string, unknown>): {
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

function getSchemaWithFts(
    fields: Array<'firstName' | 'lastName'>,
    withAttachments = false
): RxJsonSchema<HumanDocumentType> {
    const schema = clone(schemas.human) as RxJsonSchema<HumanDocumentType>;
    if (withAttachments) {
        schema.attachments = {};
    }
    fields.forEach(fieldName => {
        const fieldSchema = clone((schema.properties as Record<string, unknown>)[fieldName]) as Record<string, unknown>;
        fieldSchema.fts = {
            tokenize: 'forward',
            resolution: 9
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
        multiInstance?: boolean;
    } = {}
) {
    const db = await createRxDatabase<Collections>({
        name: input.name ?? randomToken(10),
        storage: wrappedFlexSearchStorage({
            storage: config.storage.getStorage(),
            persistence: input.persistence
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
        assert.ok(typeof persistedMeta.serialized === 'string');
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
        corrupted.serialized = 'function inject(doc){ doc.__broken(';
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
        assert.ok(typeof repairedMetaTyped.serialized === 'string');
        assert.notStrictEqual(repairedMetaTyped.serialized, corrupted.serialized);

        await second.db.remove();
    });

    it.skip('observes replication-style writes from another instance via collection-scoped changeStream', async function () {
        const name = randomToken(10);
        const first = await createDatabase({
            name,
            multiInstance: true,
            persistence: {
                minDebounce: 20,
                maxDebounce: 120
            }
        });
        const second = await createDatabase({
            name,
            multiInstance: true,
            persistence: {
                minDebounce: 20,
                maxDebounce: 120
            }
        });

        await first.collection.insert({
            passportId: 'm1',
            firstName: 'Arthur',
            lastName: 'Curry',
            age: 35
        });

        await AsyncTestUtil.waitUntil(async () => {
            const docs = await second.collection.find({
                selector: {
                    $fts: 'Arth' as never
                }
            }).exec();
            return docs.length === 1;
        }, 10 * 1000, 100);

        const replicated = await second.collection.find({
            selector: {
                $fts: 'Arth' as never
            }
        }).exec();
        assert.strictEqual(replicated.length, 1);
        assert.strictEqual(replicated[0].passportId, 'm1');

        await first.db.close();
        await second.db.close();
    });

    it('schedules debounced persistence under burst writes', async () => {
        const { db, collection } = await createDatabase({
            persistence: {
                minDebounce: 20,
                maxDebounce: 120
            }
        });

        await collection.bulkInsert([
            { passportId: 'd1', firstName: 'One', lastName: 'Writer', age: 10 },
            { passportId: 'd2', firstName: 'Two', lastName: 'Writer', age: 11 },
            { passportId: 'd3', firstName: 'Three', lastName: 'Writer', age: 12 }
        ]);

        const state = getFlexSearchState(db.name, 'humans');
        assert.ok(state);
        assert.ok((state?.changesSinceLastPersist ?? 0) >= 3);

        await waitForPersistence();

        const meta = await readMetaState(db);
        assert.ok(meta);
        assert.ok(typeof meta.serialized === 'string');
        assert.strictEqual(getFlexSearchState(db.name, 'humans')?.changesSinceLastPersist, 0);

        await db.remove();
    });
});
