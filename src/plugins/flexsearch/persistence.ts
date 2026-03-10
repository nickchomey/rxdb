/**
 * FlexSearch persistence layer.
 * Handles serialization, restoration, debounced persistence, and metadata storage.
 *
 * CRITICAL: Uses FlexSearch's serialize() API (not export()) per plan specification.
 * serialize() returns a function body string that's faster to restore than export entries.
 */

import {
    createRevision,
    flatClone,
    getDefaultRevision,
    getDefaultRxDocumentMeta,
    now
} from '../utils/index.ts';
import { decompress } from 'flexsearch';
import type { RxStorageDefaultCheckpoint } from '../../types/index.d.ts';
import type {
    FlexSearchMetaDocument,
    FlexSearchMetaDocumentData,
    FlexSearchPersistenceConfig,
    FlexSearchRuntimeState
} from './types.ts';
import { FLEXSEARCH_META_VERSION } from './types.ts';
import { catchUpFromCheckpoint } from './indexing.ts';


/**
 * Initializes the FlexSearch index state on storage instance creation.
 * Attempts to restore from serialized snapshot, falls back to rebuild if invalid.
 * Catches up any changes since the last checkpoint.
 */
export async function initializeIndexState<RxDocType, Internals, InstanceCreationOptions>(
    state: FlexSearchRuntimeState,
    instance: any,
    databaseInstanceToken: string
): Promise<void> {
    const metaDocument = await readMetaDocument(state);

    const hasCompatibleSnapshot = Boolean(
        metaDocument &&
        metaDocument.version === FLEXSEARCH_META_VERSION &&
        metaDocument.schemaHash === state.schemaHash &&
        Array.isArray(metaDocument.serialized) &&
        metaDocument.serialized.length > 0
    );

    let startCheckpoint: RxStorageDefaultCheckpoint | undefined;
    let restoredFromSnapshot = false;
    if (hasCompatibleSnapshot && metaDocument?.serialized) {
        console.log('[FlexSearch] Attempting snapshot restore, serialized length:', metaDocument.serialized.length);
        try {
            try {
                console.log('[FlexSearch] Converting serialized to Uint8Array...');
                const compressedBytes = new Uint8Array(metaDocument.serialized);
                console.log('[FlexSearch] Converted, array length:', compressedBytes.length);
            } catch (beforeDecompressError) {
                console.error('[FlexSearch] Error before decompression:', beforeDecompressError);
                throw new Error('Uint8Array conversion failed');
            }
            
            let serializedBody: string;
            try {
                // Backward compatible path for previously compressed snapshots.
                console.log('[FlexSearch] Decoding UTF-8 snapshot...');
                const decoded = new TextDecoder().decode(new Uint8Array(metaDocument.serialized));
                console.log('[FlexSearch] Decoded UTF-8, length:', decoded.length, 'first char:', decoded.charAt(0));
                serializedBody = decoded;
            } catch (decodeError) {
                console.error('[FlexSearch] UTF-8 decode failed:', decodeError instanceof Error ? decodeError.message : String(decodeError));
                throw new Error('Could not decode snapshot data');
            }
            
            console.log('[FlexSearch] Restoring index snapshot...');
            // FlexSearch serialize(false) returns raw JavaScript code like "doc.reg=..." that modifies the Document
            // instance. The code expects `doc` to be bound to the Document object.
            
            // Try multiple approaches to execute the restoration code
            let restoreFailed = true;
            
            // Approach 1: Direct eval with doc as local variable
            try {
                console.log('[FlexSearch] Attempting restoration with direct eval...');
                const doc = state.index;  // Provide 'doc' variable for the serialized code
                eval(serializedBody);
                console.log('[FlexSearch] Direct eval completed, index keys:', Object.keys(doc || {}).slice(0, 5));
                restoreFailed = false;
            } catch (e) {
                console.warn('[FlexSearch] Direct eval failed:', e instanceof Error ? e.message : String(e));
            }
            
            // Approach 2: Wrap in function with doc parameter  
            if (restoreFailed) {
                try {
                    console.log('[FlexSearch] Attempting restoration with wrapped function...');
                    const wrapper = new Function('doc', serializedBody);
                    wrapper.call(state.index, state.index);
                    console.log('[FlexSearch] Wrapped function execution completed');
                    restoreFailed = false;
                } catch (e) {
                    console.warn('[FlexSearch] Wrapped function failed:', e instanceof Error ? e.message : String(e));
                }
            }
            
            // Approach 3: Execute via function that receives doc as 'this'
            if (restoreFailed) {
                try {
                    console.log('[FlexSearch] Attempting restoration with this binding...');
                    (function (this: any) {
                        const doc = this;
                        eval(serializedBody);
                    }).call(state.index);
                    console.log('[FlexSearch] This binding approach completed');
                    restoreFailed = false;
                } catch (e) {
                    console.warn('[FlexSearch] This binding approach failed:', e instanceof Error ? e.message : String(e));
                }
            }
            
            if (restoreFailed) {
                throw new Error('All snapshot restoration approaches failed');
            }
            
            restoredFromSnapshot = true;
            console.log('[FlexSearch] Snapshot restored successfully');

            // Verify restoration worked
            try {
                const testResult = state.index.search('test');
                console.log('[FlexSearch] Post-restore verification: test search returned', 
                    Array.isArray(testResult) ? testResult.length + ' results' :
                    typeof testResult);
            } catch (e) {
                console.error('[FlexSearch] Error testing restored index:', e instanceof Error ? e.message : String(e));
                // Don't throw - if index doesn't have data, that's okay for "test" search
                // The real test will be when we do the incremental catch-up
            }
        } catch (error) {
            // Snapshot is optional; if restore fails we rebuild by catch-up/indexing path below.
            console.error('[FlexSearch] Outer catch - snapshot restore failed:', error instanceof Error ? error.message : String(error));
            restoredFromSnapshot = false;
        }

        if (restoredFromSnapshot && metaDocument.checkpointId && typeof metaDocument.checkpointLwt === 'number') {
            startCheckpoint = {
                id: metaDocument.checkpointId,
                lwt: metaDocument.checkpointLwt
            };
            state.checkpoint = startCheckpoint;
            console.log('[FlexSearch] Restored checkpoint:', startCheckpoint);
        }
    }

    // CRITICAL FIX: Only do full catchup if snapshot restore failed or didn't exist.
    // If we restored from snapshot, only catch up changes AFTER the snapshot checkpoint.
    // This prevents rebuilding the entire index and interfering with the restored state.
    console.log('[FlexSearch] Starting catchup, restoredFromSnapshot:', restoredFromSnapshot, 'startCheckpoint:', startCheckpoint);
    const caughtUpChanges = await catchUpFromCheckpoint(
        state,
        instance,
        restoredFromSnapshot ? startCheckpoint : undefined
    );
    console.log('[FlexSearch] Catchup complete, had changes:', caughtUpChanges);

    if (!restoredFromSnapshot || caughtUpChanges) {
        await persistIndexSnapshot(state, databaseInstanceToken);
    }
}


/**
 * Schedules debounced persistence after write operations.
 * Uses adaptive timing based on actual serialization duration.
 */
export function schedulePersistence(
    state: FlexSearchRuntimeState,
    persistence: FlexSearchPersistenceConfig,
    databaseInstanceToken: string
): void {
    if (!state.metaStorage) {
        return;
    }

    const config = {
        minDebounce: persistence?.minDebounce ?? 1000,
        maxDebounce: persistence?.maxDebounce ?? 10000,
        adaptive: persistence?.adaptive ?? true
    };
    const currentTime = now();
    if (!state.firstChangeAt) {
        state.firstChangeAt = currentTime;
    }

    const activeDebounce = state.dynamicDebounceMs ?? config.minDebounce;
    const elapsed = currentTime - state.firstChangeAt;
    const remainingMax = config.maxDebounce - elapsed;
    const delay = remainingMax <= 0 ? 0 : Math.min(activeDebounce, remainingMax);

    if (state.persistenceTimer) {
        clearTimeout(state.persistenceTimer);
    }
    state.persistenceTimer = setTimeout(() => {
        const startedAt = now();
        void enqueueStateWork(state, async () => {
            await persistIndexSnapshot(state, databaseInstanceToken);
            if (config.adaptive) {
                const workDuration = Math.max(5, now() - startedAt);
                state.dynamicDebounceMs = Math.max(
                    config.minDebounce,
                    Math.min(config.maxDebounce, workDuration * 8)
                );
            }
        });
    }, delay);
}


/**
 * Enqueues async work to prevent concurrent operations.
 * All write queue operations run serially.
 */
export function enqueueStateWork(
    state: FlexSearchRuntimeState,
    work: () => Promise<void>
): Promise<void> {
    const current = state.writeQueue ?? Promise.resolve();
    const next = current.then(work).catch(error => {
        console.error('[FlexSearch] queued work failed', error);
    });
    state.writeQueue = next;
    return next;
}

/**
 * Reads the FlexSearch metadata document from storage.
 */
async function readMetaDocument(
    state: FlexSearchRuntimeState
): Promise<FlexSearchMetaDocumentData | undefined> {
    if (!state.metaStorage) {
        return undefined;
    }
    const docs = await state.metaStorage.findDocumentsById([state.metaDocumentId], false);
    const doc = docs[0] as FlexSearchMetaDocumentData | undefined;
    if (!doc || doc._deleted) {
        return undefined;
    }
    return doc;
}

/**
 * Writes a FlexSearch metadata document to storage.
 * Handles revision generation and conflict resolution.
 */
async function writeMetaDocument(
    state: FlexSearchRuntimeState,
    databaseInstanceToken: string,
    patch: Partial<FlexSearchMetaDocument>
): Promise<void> {
    if (!state.metaStorage) {
        return;
    }

    const previous = await readMetaDocument(state);
    const nextDocument: FlexSearchMetaDocumentData = previous ? flatClone(previous) : {
        id: state.metaDocumentId,
        version: FLEXSEARCH_META_VERSION,
        timestamp: now(),
        schemaHash: state.schemaHash,
        _attachments: {},
        _deleted: false,
        _meta: getDefaultRxDocumentMeta(),
        _rev: getDefaultRevision()
    };

    Object.assign(nextDocument, patch);
    nextDocument.id = state.metaDocumentId;
    nextDocument.version = FLEXSEARCH_META_VERSION;
    nextDocument.timestamp = now();
    nextDocument.schemaHash = state.schemaHash;
    nextDocument._deleted = false;
    nextDocument._meta.lwt = now();
    nextDocument._rev = createRevision(databaseInstanceToken, previous);

    const writeResult = await state.metaStorage.bulkWrite([
        {
            previous,
            document: nextDocument
        }
    ], 'flexsearch-meta-write');
    if (writeResult.error.length > 0) {
        throw writeResult.error[0];
    }
}

/**
 * Persists the current index snapshot to metadata storage.
 * Updates checkpoint and resets persistence counters.
 */
export async function persistIndexSnapshot(
    state: FlexSearchRuntimeState,
    databaseInstanceToken: string
): Promise<void> {
    const serializedBody = state.index.serialize(false);
    const persistedBytes = new TextEncoder().encode(serializedBody);

    await writeMetaDocument(state, databaseInstanceToken, {
        serialized: Array.from(persistedBytes),
        checkpointId: state.checkpoint?.id,
        checkpointLwt: state.checkpoint?.lwt
    });
    state.changesSinceLastPersist = 0;
    state.firstChangeAt = undefined;
}
