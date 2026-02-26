import assert from 'assert';
import config, { describeParallel } from './config.ts';

import {
    schemaObjects,
    schemas,
} from '../../plugins/test-utils/index.mjs';
import {
    clone,
    createRxDatabase,
    randomToken,
    addRxPlugin,
    createBlob,
    blobToString,
    RxCollection,
    RxDatabase
} from '../../plugins/core/index.mjs';
import { RxDBDumpFilePlugin, importDatabase } from '../../plugins/dump-file/index.mjs';
addRxPlugin(RxDBDumpFilePlugin);

describeParallel('dump-file.test.ts', () => {
    if (!config.storage.hasAttachments) {
        return;
    }

    function getHumanSchemaWithAttachments() {
        const schemaJson = clone(schemas.human);
        schemaJson.attachments = {};
        return schemaJson;
    }

    async function createCollectionWithDocs(
        docCount: number,
        opts?: { attachments?: boolean; }
    ): Promise<{ db: RxDatabase, col: RxCollection; }> {
        const db = await createRxDatabase({
            name: randomToken(10),
            storage: config.storage.getStorage(),
            multiInstance: false,
            ignoreDuplicate: true
        });
        const collections = await db.addCollections({
            humans: { schema: getHumanSchemaWithAttachments() }
        });
        const col = collections.humans;

        for (let i = 0; i < docCount; i++) {
            const docData = schemaObjects.humanData();
            if (opts?.attachments) {
                (docData as any)._attachments = [
                    {
                        id: 'file' + i + '.txt',
                        type: 'text/plain',
                        data: createBlob('content for doc ' + i, 'text/plain')
                    }
                ];
            }
            await col.insert(docData);
        }
        return { db, col };
    }

    describe('collection export/import', () => {
        it('empty collection roundtrip', async () => {
            const { db: db1, col } = await createCollectionWithDocs(0);
            const blob = await (db1 as any).export({ collections: [col.name] });
            assert.ok(blob instanceof Blob);
            assert.ok(blob.size > 0);

            // Import into fresh collection
            const { db: db2, col: col2 } = await createCollectionWithDocs(0);
            await (db2 as any).import(blob);

            const docs = await col2.find().exec();
            assert.strictEqual(docs.length, 0);

            db1.close();
            db2.close();
        });
        it('docs without attachments roundtrip', async () => {
            const { db: db1, col } = await createCollectionWithDocs(0);
            const docData = schemaObjects.humanData();
            docData.firstName = 'DumpTest';
            await col.insert(docData);

            const blob = await (db1 as any).export({ collections: [col.name] });

            const { db: db2, col: col2 } = await createCollectionWithDocs(0);
            await (db2 as any).import(blob);

            const docs = await col2.find().exec();
            assert.strictEqual(docs.length, 1);
            assert.strictEqual(docs[0].firstName, 'DumpTest');

            db1.close();
            db2.close();
        });
        it('docs with attachments — byte-identical roundtrip', async () => {
            const { db: db1, col } = await createCollectionWithDocs(0);
            const docData = schemaObjects.humanData();
            (docData as any)._attachments = [
                {
                    id: 'hello.txt',
                    type: 'text/plain',
                    data: createBlob('hello dump-file', 'text/plain')
                }
            ];
            await col.insert(docData);

            const blob = await (db1 as any).export({ collections: [col.name] });

            const { db: db2, col: col2 } = await createCollectionWithDocs(0);
            await (db2 as any).import(blob);

            const docs = await col2.find().exec();
            assert.strictEqual(docs.length, 1);

            const att = docs[0].getAttachment('hello.txt');
            assert.ok(att);
            assert.strictEqual(att.type, 'text/plain');
            const data = await att.getData();
            const text = await blobToString(data);
            assert.strictEqual(text, 'hello dump-file');

            db1.close();
            db2.close();
        });
        it('export without attachments option', async () => {
            const { db: db1, col } = await createCollectionWithDocs(1, { attachments: true });

            const blob = await (db1 as any).export({ collections: [col.name], attachments: false });

            const { db: db2, col: col2 } = await createCollectionWithDocs(0);
            await (db2 as any).import(blob);

            const docs = await col2.find().exec();
            assert.strictEqual(docs.length, 1);
            // No attachments should exist
            const allAtts = docs[0].allAttachments();
            assert.strictEqual(allAtts.length, 0);

            db1.close();
            db2.close();
        });
    });

    describe('database export/import', () => {
        it('multi-collection database roundtrip with auto-create', async () => {
            const db1 = await createRxDatabase({
                name: randomToken(10),
                storage: config.storage.getStorage(),
                multiInstance: false,
                ignoreDuplicate: true
            });
            const schemaJson = getHumanSchemaWithAttachments();
            await db1.addCollections({
                humans: { schema: schemaJson },
                humans2: { schema: schemaJson }
            });

            const doc1Data = schemaObjects.humanData();
            doc1Data.firstName = 'Col1Doc';
            (doc1Data as any)._attachments = [
                {
                    id: 'col1.txt',
                    type: 'text/plain',
                    data: createBlob('collection 1', 'text/plain')
                }
            ];
            await db1.humans.insert(doc1Data);

            const doc2Data = schemaObjects.humanData();
            doc2Data.firstName = 'Col2Doc';
            await db1.humans2.insert(doc2Data);

            const blob = await (db1 as any).export();

            // Import into fresh DB — collections should be auto-created
            const db2 = await createRxDatabase({
                name: randomToken(10),
                storage: config.storage.getStorage(),
                multiInstance: false,
                ignoreDuplicate: true
            });
            await (db2 as any).import(blob);

            // Verify collections were auto-created
            assert.ok(db2.collections['humans']);
            assert.ok(db2.collections['humans2']);

            const col1Docs = await db2.humans.find().exec();
            assert.strictEqual(col1Docs.length, 1);
            assert.strictEqual(col1Docs[0].firstName, 'Col1Doc');
            const att = col1Docs[0].getAttachment('col1.txt');
            assert.ok(att);
            const text = await blobToString(await att.getData());
            assert.strictEqual(text, 'collection 1');

            const col2Docs = await db2.humans2.find().exec();
            assert.strictEqual(col2Docs.length, 1);
            assert.strictEqual(col2Docs[0].firstName, 'Col2Doc');

            db1.close();
            db2.close();
        });
        it('selective collection export', async () => {
            const db = await createRxDatabase({
                name: randomToken(10),
                storage: config.storage.getStorage(),
                multiInstance: false,
                ignoreDuplicate: true
            });
            const schemaJson = getHumanSchemaWithAttachments();
            await db.addCollections({
                humans: { schema: schemaJson },
                humans2: { schema: schemaJson }
            });

            await db.humans.insert(schemaObjects.humanData());
            await db.humans2.insert(schemaObjects.humanData());

            // Only export humans collection
            const blob = await (db as any).export({ collections: ['humans'] });

            // Import into fresh DB — only 'humans' should be auto-created
            const db2 = await createRxDatabase({
                name: randomToken(10),
                storage: config.storage.getStorage(),
                multiInstance: false,
                ignoreDuplicate: true
            });
            await (db2 as any).import(blob);

            assert.ok(db2.collections['humans']);
            assert.ok(!db2.collections['humans2']);

            const col1Docs = await db2.humans.find().exec();
            assert.strictEqual(col1Docs.length, 1);

            db.close();
            db2.close();
        });
        it('import into existing db preserves pre-existing collections', async () => {
            const schemaJson = getHumanSchemaWithAttachments();

            // Source DB with one collection
            const db1 = await createRxDatabase({
                name: randomToken(10),
                storage: config.storage.getStorage(),
                multiInstance: false,
                ignoreDuplicate: true
            });
            await db1.addCollections({ humans: { schema: schemaJson } });
            const srcDoc = schemaObjects.humanData();
            srcDoc.firstName = 'FromExport';
            await db1.humans.insert(srcDoc);
            const blob = await (db1 as any).export();

            // Target DB already has 'humans' with existing data
            const db2 = await createRxDatabase({
                name: randomToken(10),
                storage: config.storage.getStorage(),
                multiInstance: false,
                ignoreDuplicate: true
            });
            await db2.addCollections({ humans: { schema: schemaJson } });
            const existingDoc = schemaObjects.humanData();
            existingDoc.firstName = 'PreExisting';
            await db2.humans.insert(existingDoc);

            // Import — default overwriteOnConflict=true, but these are different docs (different primary keys)
            await (db2 as any).import(blob);

            const docs = await db2.humans.find().exec();
            assert.strictEqual(docs.length, 2);

            db1.close();
            db2.close();
        });
    });

    describe('conflict handling', () => {
        it('overwrite on conflict (default)', async () => {
            const { db: db1, col } = await createCollectionWithDocs(0);
            const docData = schemaObjects.humanData();
            docData.firstName = 'FromExport';
            await col.insert(docData);

            const blob = await (db1 as any).export({ collections: [col.name] });

            const { db: db2, col: col2 } = await createCollectionWithDocs(0);
            const existingData = Object.assign({}, docData, { firstName: 'Existing' });
            await col2.insert(existingData);

            // Import with default overwriteOnConflict=true — should replace
            await (db2 as any).import(blob);

            const docs = await col2.find().exec();
            assert.strictEqual(docs.length, 1);
            assert.strictEqual(docs[0].firstName, 'FromExport');

            db1.close();
            db2.close();
        });
        it('skip on conflict when overwriteOnConflict=false', async () => {
            const { db: db1, col } = await createCollectionWithDocs(0);
            const docData = schemaObjects.humanData();
            docData.firstName = 'Original';
            await col.insert(docData);

            const blob = await (db1 as any).export({ collections: [col.name] });

            // Modify the doc in the target
            const { db: db2, col: col2 } = await createCollectionWithDocs(0);
            const importData = Object.assign({}, docData, { firstName: 'AlreadyHere' });
            await col2.insert(importData);

            // Import with overwriteOnConflict=false — should not overwrite
            await (db2 as any).import(blob, { overwriteOnConflict: false });

            const docs = await col2.find().exec();
            assert.strictEqual(docs.length, 1);
            assert.strictEqual(docs[0].firstName, 'AlreadyHere');

            db1.close();
            db2.close();
        });
    });

    describe('standalone import', () => {
        it('import from blob without pre-creating database or collections', async () => {
            // Create source DB with data
            const db1 = await createRxDatabase({
                name: randomToken(10),
                storage: config.storage.getStorage(),
                multiInstance: false,
                ignoreDuplicate: true
            });
            const schemaJson = getHumanSchemaWithAttachments();
            await db1.addCollections({
                humans: { schema: schemaJson }
            });
            const docData = schemaObjects.humanData();
            docData.firstName = 'StandaloneTest';
            (docData as any)._attachments = [
                {
                    id: 'standalone.txt',
                    type: 'text/plain',
                    data: createBlob('standalone content', 'text/plain')
                }
            ];
            await db1.humans.insert(docData);
            const blob = await (db1 as any).export();

            // Remove source db before standalone import to avoid same-name collision in memory storage
            await db1.remove();

            // Import using standalone function — no db or collections pre-created
            const db2 = await importDatabase(blob, {
                storage: config.storage.getStorage()
            });

            // Verify database and collections exist
            assert.ok(db2);
            assert.ok(db2.collections['humans']);

            const docs = await db2.humans.find().exec();
            assert.strictEqual(docs.length, 1);
            assert.strictEqual(docs[0].firstName, 'StandaloneTest');

            const att = docs[0].getAttachment('standalone.txt');
            assert.ok(att);
            const text = await blobToString(await att.getData());
            assert.strictEqual(text, 'standalone content');

            db2.close();
        });
    });

    describe('export → remove → import roundtrip', () => {
        it('export db, destroy it, import into new db, verify identical data', async () => {
            // Create source DB with docs and attachments
            const db1 = await createRxDatabase({
                name: randomToken(10),
                storage: config.storage.getStorage(),
                multiInstance: false,
                ignoreDuplicate: true
            });
            const schemaJson = getHumanSchemaWithAttachments();
            await db1.addCollections({
                humans: { schema: schemaJson }
            });

            const docData = schemaObjects.humanData();
            docData.firstName = 'RoundtripTest';
            (docData as any)._attachments = [
                {
                    id: 'roundtrip.txt',
                    type: 'text/plain',
                    data: createBlob('roundtrip content', 'text/plain')
                }
            ];
            await db1.humans.insert(docData);

            const docData2 = schemaObjects.humanData();
            docData2.firstName = 'SecondDoc';
            await db1.humans.insert(docData2);

            // Snapshot the original data for comparison
            const originalDocs = await db1.humans.find().exec();
            const originalData = originalDocs.map((d: any) => ({
                passportId: d.passportId,
                firstName: d.firstName,
                lastName: d.lastName,
                age: d.age,
                attachmentIds: d.allAttachments().map((a: any) => a.id).sort()
            })).sort((a: any, b: any) => a.passportId.localeCompare(b.passportId));

            // Get original attachment content
            const originalAttDoc = originalDocs.find((d: any) => d.firstName === 'RoundtripTest')!;
            const originalAtt = originalAttDoc.getAttachment('roundtrip.txt');
            const originalAttText = await blobToString(await originalAtt!.getData());

            // Export
            const blob = await (db1 as any).export();
            assert.ok(blob instanceof Blob);

            // Destroy the original database (deletes storage)
            await db1.remove();

            // Import into new db from the blob
            const db2 = await importDatabase(blob, {
                storage: config.storage.getStorage()
            });

            // Verify collections were created
            assert.ok(db2.collections['humans']);

            // Snapshot the restored data
            const restoredDocs = await db2.humans.find().exec();
            const restoredData = restoredDocs.map((d: any) => ({
                passportId: d.passportId,
                firstName: d.firstName,
                lastName: d.lastName,
                age: d.age,
                attachmentIds: d.allAttachments().map((a: any) => a.id).sort()
            })).sort((a: any, b: any) => a.passportId.localeCompare(b.passportId));

            // Compare document data (excluding internal fields like _rev, _meta)
            assert.deepStrictEqual(restoredData, originalData);

            // Compare attachment content
            const restoredAttDoc = restoredDocs.find((d: any) => d.firstName === 'RoundtripTest')!;
            const restoredAtt = restoredAttDoc.getAttachment('roundtrip.txt');
            assert.ok(restoredAtt);
            const restoredAttText = await blobToString(await restoredAtt!.getData());
            assert.strictEqual(restoredAttText, originalAttText);

            db2.close();
        });
    });
});
