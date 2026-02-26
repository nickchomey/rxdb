/**
 * Streaming binary export/import plugin for RxDB databases and collections.
 * Produces a single gzip-compressed file containing documents + attachment blobs.
 *
 * Binary format (inside the gzip stream):
 *   [4B magic "RXDB"] [4B version uint32=1]
 *   [4B+NB meta JSON] — { dbName, collections: [{ name, schema, schemaHash }] }
 *   Per collection:
 *     [4B+NB collectionName] [4B docCount uint32]
 *     Per document:
 *       [4B+NB doc JSON (stubs only in _attachments)]
 *       [2B attachmentCount uint16]
 *       Per attachment:
 *         [2B+NB attachmentId]
 *         [4B+NB raw blob bytes]
 *   [4B 0x00000000 end marker]
 */
import {
    createRxQuery,
    queryCollection,
    _getDefaultQuery
} from '../../rx-query.ts';
import {
    newRxError
} from '../../rx-error.ts';
import {
    createRxDatabase
} from '../../rx-database.ts';

import type {
    RxDatabase,
    RxCollection,
    RxPlugin,
    RxDocumentData,
    RxStorage
} from '../../types/index.d.ts';
import {
    flatClone,
    getDefaultRevision,
    now
} from '../../plugins/utils/index.ts';

const MAGIC = new Uint8Array([0x52, 0x58, 0x44, 0x42]); // "RXDB"
const FORMAT_VERSION = 1;

/** Byte widths for binary headers */
const UINT16 = 2 as const;
const UINT32 = 4 as const;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

const INTERNAL_FIELDS = ['_rev', '_attachments', '_deleted', '_meta'];

// ---- Export ----

export interface DumpFileExportOptions {
    /** Which collections to include (default: all) */
    collections?: string[];
    /** Whether to include attachments (default: true) */
    attachments?: boolean;
}

/**
 * Export a database to a compressed Blob.
 */
async function exportDatabase(
    this: RxDatabase,
    options?: DumpFileExportOptions
): Promise<Blob> {
    const useCollections = Object.keys(this.collections)
        .filter(colName => !options?.collections || options.collections.includes(colName))
        .filter(colName => colName.charAt(0) !== '_')
        .map(colName => this.collections[colName]);

    return buildExportBlob(useCollections, this.name, options);
}

async function buildExportBlob(
    collections: RxCollection[],
    dbName: string,
    options?: DumpFileExportOptions
): Promise<Blob> {
    const includeAttachments = options?.attachments !== false;

    // Build meta — includes full schemas so import can auto-create collections
    const meta: any = {
        dbName,
        collections: []
    };
    for (const col of collections) {
        meta.collections.push({
            name: col.name,
            schema: stripInternalSchemaFields(col.schema.jsonSchema),
            schemaHash: await col.schema.hash
        });
    }

    const allChunks: Uint8Array[] = [];

    // Magic + version
    allChunks.push(MAGIC);
    allChunks.push(writeUint(FORMAT_VERSION, UINT32));

    // Meta
    allChunks.push(writeLengthPrefixed(textEncoder.encode(JSON.stringify(meta)), UINT32));

    // Collections
    for (const col of collections) {
        const colChunks = await exportCollectionToChunks(col, includeAttachments);
        allChunks.push(...colChunks);
    }

    // End marker
    allChunks.push(writeUint(0, UINT32));

    return concatAndCompress(allChunks);
}

/**
 * Strip internal fields that fillWithDefaultSettings adds,
 * so the schema can be passed back to addCollections().
 */
function stripInternalSchemaFields(schema: any): any {
    const stripped = flatClone(schema);
    stripped.properties = flatClone(stripped.properties);
    for (const field of INTERNAL_FIELDS) {
        delete stripped.properties[field];
    }
    if (Array.isArray(stripped.required)) {
        stripped.required = stripped.required.filter(
            (f: string) => !INTERNAL_FIELDS.includes(f)
        );
    }
    // Remove the normalized indexes — addCollections will re-create them
    delete stripped.indexes;
    // Remove additionalProperties (added by fillWithDefaultSettings)
    delete stripped.additionalProperties;
    return stripped;
}

async function exportCollectionToChunks(
    collection: RxCollection,
    includeAttachments: boolean
): Promise<Uint8Array[]> {
    const chunks: Uint8Array[] = [];

    // Collection name
    chunks.push(writeLengthPrefixed(textEncoder.encode(collection.name), UINT32));

    // Query all docs
    const query = createRxQuery('find', _getDefaultQuery(), collection);
    const result = await queryCollection(query);
    const docs = result.docs;
    // Doc count
    chunks.push(writeUint(docs.length, UINT32));

    for (const docData of docs) {
        const doc = flatClone(docData);
        const attachmentEntries = Object.entries((doc as any)._attachments || {});

        // Write doc JSON with attachment stubs (remove .data from _attachments)
        if (includeAttachments && attachmentEntries.length > 0) {
            const stubAttachments: any = {};
            for (const [attId, attData] of attachmentEntries) {
                stubAttachments[attId] = {
                    length: (attData as any).length,
                    type: (attData as any).type,
                    digest: (attData as any).digest
                };
            }
            (doc as any)._attachments = stubAttachments;
        }

        const docJson = JSON.stringify(doc);
        chunks.push(writeLengthPrefixed(textEncoder.encode(docJson), UINT32));

        // Attachment count
         if (includeAttachments) {
            chunks.push(writeUint(attachmentEntries.length, UINT16));

            for (const [attId, attData] of attachmentEntries) {
                // Attachment ID
                chunks.push(writeLengthPrefixed(textEncoder.encode(attId), UINT16));

                // Attachment blob data
                const primaryPath = collection.schema.primaryPath;
                const documentId = (docData as any)[primaryPath];
                const blob = await collection.storageInstance.getAttachmentData(
                    documentId,
                    attId,
                    (attData as any).digest
                );
                const arrayBuffer = await blob.arrayBuffer();
                chunks.push(writeLengthPrefixed(new Uint8Array(arrayBuffer), UINT32));
            }
        } else {
            chunks.push(writeUint(0, UINT16));
        }
    }

    return chunks;
}

// ---- File save helpers ----

/**
 * Save a Blob to the filesystem using the appropriate environment API.
 * Exported so callers can compose: `const blob = await db.export(); await saveToFile(blob, 'path');`
 *
 * - Node.js / Bun: uses node:fs/promises writeFile
 * - Deno: uses Deno.writeFile
 * - Browser: uses File System Access API (showSaveFilePicker) with anchor-download fallback
 */
export async function saveToFile(blob: Blob, filePath: string): Promise<void> {
    if (typeof process !== 'undefined' && process.versions?.node) {
        // Node.js / Bun
        const { writeFile } = await import('node:fs/promises');
        const arrayBuffer = await blob.arrayBuffer();
        await writeFile(filePath, new Uint8Array(arrayBuffer));
    } else if (typeof (globalThis as any).Deno !== 'undefined') {
        // Deno
        const data = new Uint8Array(await blob.arrayBuffer());
        await (globalThis as any).Deno.writeFile(filePath, data);
    } else if (typeof window !== 'undefined' && 'showSaveFilePicker' in window) {
        // Browser — File System Access API
        const handle = await (window as any).showSaveFilePicker({
            suggestedName: filePath,
            types: [{
                description: 'RxDB dump file',
                accept: { 'application/gzip': ['.gz', '.rxdb'] }
            }]
        });
        const writable = await handle.createWritable();
        await writable.write(blob);
        await writable.close();
    } else if (typeof document !== 'undefined') {
        // Browser fallback — anchor download
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filePath;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    } else {
        throw new Error('dump-file: unable to save file — unsupported environment');
    }
}

function concatAndCompress(chunks: Uint8Array[]): Promise<Blob> {
    const totalSize = chunks.reduce((sum, c) => sum + c.length, 0);
    const raw = new Uint8Array(totalSize);
    let offset = 0;
    for (const chunk of chunks) {
        raw.set(chunk, offset);
        offset += chunk.length;
    }
    const compressedStream = new Blob([raw]).stream().pipeThrough(
        new CompressionStream('gzip')
    );
    return new Response(compressedStream).blob();
}

// ---- Import ----

export interface DumpFileImportOptions {
    /**
     * Whether to overwrite existing documents on conflict.
     * Defaults to true.
     */
    overwriteOnConflict?: boolean;
    /** Batch size for bulkWrite (default: 100) */
    batchSize?: number;
    /**
     * The RxStorage to use when creating a new database via standalone importFromFile().
     * Not needed when importing into an existing database.
     */
    storage?: RxStorage<any, any>;
}

/**
 * Import a dump Blob.
 *
 * As a standalone function — importDatabase(blob, { storage }):
 *   Reads the data
ase name and collection schemas from the export header,, *   creates the database, then imports all data.
 *
 * As a prototype method — db.import(blob, options?):
 *   Imports into the existing database. Missing collections are auto-created
 *   from the schemas stored in the header.
 *
 * `storage` is only required when called standalone.
 */
export async function importDatabase(
    this: RxDatabase | undefined,
    blob: Blob,
    options: DumpFileImportOptions = {}
): Promise<RxDatabase> {
    const reader = new BinaryReader(
        blob.stream().pipeThrough(new DecompressionStream('gzip'))
    );
    const meta = await readAndValidateHeader(reader);

    let db: RxDatabase;
    if (this) {
        db = this;
    } else {
        if (!options.storage) {
            throw new Error('dump-file: importDatabase requires options.storage when called standalone');
        }
        db = await createRxDatabase({
            name: meta.dbName,
            storage: options.storage,
            multiInstance: false,
            ignoreDuplicate: true
        });
    }
    for (const colMeta of meta.collections) {
        if (!db.collections[colMeta.name]) {
            if (!colMeta.schema) {
                throw newRxError('JD1', { missingCollections: [colMeta.name] });
            }
            await db.addCollections({
                [colMeta.name]: { schema: colMeta.schema }
            });
        }
    }
    for (const colMeta of meta.collections) {
        await importCollectionData(reader, db.collections[colMeta.name], options);
    }

    reader.release();
    return db;
}

async function readAndValidateHeader(reader: BinaryReader): Promise<any> {
    const magic = await reader.readBytes(MAGIC.length);
    if (magic.length !== MAGIC.length || !magic.every((b, i) => b === MAGIC[i])) {
        throw new Error('dump-file: invalid magic bytes');
    }
    const version = await reader.readUint(UINT32);
    if (version !== FORMAT_VERSION) {
        throw new Error('dump-file: unsupported format version ' + version);
    }
    const metaStr = await reader.readLengthPrefixed(UINT32, true);
    return JSON.parse(metaStr);
}

async function importCollectionData(
    reader: BinaryReader,
    collection: RxCollection,
    options?: DumpFileImportOptions
): Promise<void> {
    const batchSize = options?.batchSize ?? 100;
    const overwrite = options?.overwriteOnConflict !== false;

    // Collection name
    const _colName = await reader.readLengthPrefixed(UINT32, true);

    // Doc count
    const docCount = await reader.readUint(UINT32);

    let batch: any[] = [];

    for (let i = 0; i < docCount; i++) {
        // Read doc JSON
        const docStr = await reader.readLengthPrefixed(UINT32, true);
        const docData = JSON.parse(docStr);

        // Read attachment count
        const attCount = await reader.readUint(UINT16);

        // Read attachments
        const attachments: { [id: string]: any } = {};
        for (let a = 0; a < attCount; a++) {
            const attId = await reader.readLengthPrefixed(UINT16, true);
            const attBytes = await reader.readLengthPrefixed(UINT32);
            const stubData = docData._attachments?.[attId];
            attachments[attId] = {
                length: attBytes.length,
                type: stubData?.type || 'application/octet-stream',
                digest: stubData?.digest || '',
                data: new Blob([attBytes.buffer as ArrayBuffer], { type: stubData?.type || 'application/octet-stream' })
            };
        }

        // Build write document
        const document: RxDocumentData<any> = Object.assign(
            {},
            docData,
            {
                _meta: {
                    lwt: now()
                },
                _rev: getDefaultRevision(),
                _attachments: attachments,
                _deleted: false
            }
        );

        batch.push({ document });

        if (batch.length >= batchSize) {
            await writeBatch(collection, batch, overwrite);
            batch = [];
        }
    }

    if (batch.length > 0) {
        await writeBatch(collection, batch, overwrite);
    }
}

async function writeBatch(
    collection: RxCollection,
    batch: any[],
    overwrite: boolean
): Promise<void> {
    const result = await collection.storageInstance.bulkWrite(
        batch,
        'dump-file-import'
    );

    if (overwrite && result.error.length > 0) {
        const retries = result.error
            .filter((err: any) => err.status === 409)
            .map((err: any) => {
                const original = batch.find(
                    (b: any) => b.document[collection.schema.primaryPath] === err.documentId
                );
                if (!original) {
                    return null;
                }
                return {
                    document: Object.assign({}, original.document, {
                        _rev: err.documentInDb._rev,
                        _meta: err.documentInDb._meta
                    }),
                    previous: err.documentInDb
                };
            })
            .filter((x: any): x is NonNullable<typeof x> => x !== null);

        if (retries.length > 0) {
            await collection.storageInstance.bulkWrite(retries, 'dump-file-import-overwrite');
        }
    }
}

// ---- Binary helpers ----
/**
 * Write a length-prefixed byte array.
 * @param data - The payload bytes
 * @param headerBytes - 2 for uint16 length prefix, 4 for uint32
 */
function writeLengthPrefixed(data: Uint8Array, headerBytes: 2 | 4): Uint8Array {
    const header = writeUint(data.length, headerBytes);
    const combined = new Uint8Array(headerBytes + data.length);
    combined.set(header);
    combined.set(data, headerBytes);
    return combined;
}

/**
 * Write an unsigned integer as big-endian bytes.
 * @param value - The integer value
 * @param bytes - 2 for uint16, 4 for uint32
 */
function writeUint(value: number, bytes: 2 | 4): Uint8Array {
    const buf = new Uint8Array(bytes);
    const view = new DataView(buf.buffer);
    if (bytes === 2) {
        view.setUint16(0, value, false);
    } else {
        view.setUint32(0, value, false);
    }
    return buf;
}

// ---- Incremental binary reader ----

class BinaryReader {
    private buffer: Uint8Array = new Uint8Array(0);
    private reader: ReadableStreamDefaultReader<Uint8Array>;
    private done = false;

    constructor(stream: ReadableStream<Uint8Array>) {
        this.reader = stream.getReader();
    }

    private async fill(needed: number): Promise<void> {
        while (this.buffer.length < needed && !this.done) {
            const { value, done } = await this.reader.read();
            if (done) {
                this.done = true;
                return;
            }
            const merged = new Uint8Array(this.buffer.length + value.length);
            merged.set(this.buffer);
            merged.set(value, this.buffer.length);
            this.buffer = merged;
        }
    }

    async readBytes(n: number): Promise<Uint8Array> {
        await this.fill(n);
        if (this.buffer.length < n) {
            throw new Error('dump-file: unexpected end of stream');
        }
        const result = this.buffer.slice(0, n);
        this.buffer = this.buffer.slice(n);
        return result;
    }

    async readUint(bytes: 2 | 4): Promise<number> {
        const data = await this.readBytes(bytes);
        const view = new DataView(data.buffer, data.byteOffset, bytes);
        return bytes === 2
            ? view.getUint16(0, false)
            : view.getUint32(0, false);
    }

    async readLengthPrefixed(headerBytes: 2 | 4, asString: true): Promise<string>;
    async readLengthPrefixed(headerBytes: 2 | 4, asString?: false): Promise<Uint8Array>;
    async readLengthPrefixed(headerBytes: 2 | 4, asString?: boolean): Promise<Uint8Array | string> {
        const len = await this.readUint(headerBytes);
        const bytes = await this.readBytes(len);
        return asString ? textDecoder.decode(bytes) : bytes;
    }

    release(): void {
        this.reader.releaseLock();
    }
}

// ---- Plugin registration ----

export const RxDBDumpFilePlugin: RxPlugin = {
    name: 'dump-file',
    rxdb: true,
    prototypes: {
        RxDatabase: (proto: any) => {
            proto.export = exportDatabase;
            proto.import = importDatabase;
        }
    },
    overwritable: {}
};

