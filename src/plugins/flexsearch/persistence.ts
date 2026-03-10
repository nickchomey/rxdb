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


function logDebug(state: FlexSearchRuntimeState, message: string): void {
    if (!state.debug) {
        return;
    }
    console.log(`[FlexSearch Example][${state.collectionName}] ${message}`);
}


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
    const initStartedAt = now();
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
        try {
            const restoreStartedAt = now();
            logDebug(
                state,
                `Restoring persisted snapshot (${metaDocument.serialized.length.toLocaleString()} bytes) from metadata storage`
            );

            // Decode persisted snapshot
            const serializedBody = new TextDecoder().decode(new Uint8Array(metaDocument.serialized));

            // Restore index by executing serialized code with doc parameter
            // serialize(false) returns JavaScript code like "doc.reg=new Set(...)"
            const wrapper = new Function('doc', serializedBody);
            wrapper.call(state.index, state.index);
            restoredFromSnapshot = true;
            logDebug(
                state,
                `Snapshot restored and injected in ${(now() - restoreStartedAt).toFixed(2)}ms`
            );
        } catch (error) {
            // Snapshot is optional; if restore fails we rebuild by catch-up/indexing path below.
            restoredFromSnapshot = false;
            logDebug(state, `Snapshot restore failed, rebuilding index from storage changes`);
        }

        if (restoredFromSnapshot && metaDocument.checkpointId && typeof metaDocument.checkpointLwt === 'number') {
            startCheckpoint = {
                id: metaDocument.checkpointId,
                lwt: metaDocument.checkpointLwt
            };
            state.checkpoint = startCheckpoint;
        }
    }

    // If we restored from snapshot, only catch up changes AFTER the snapshot checkpoint.
    // This prevents rebuilding the entire index and interfering with the restored state.
    const caughtUpChanges = await catchUpFromCheckpoint(
        state,
        instance,
        restoredFromSnapshot ? startCheckpoint : undefined
    );

    if (restoredFromSnapshot) {
        logDebug(
            state,
            `Snapshot injection complete after catch-up in ${(now() - initStartedAt).toFixed(2)}ms` +
            (caughtUpChanges ? ' (applied post-snapshot changes)' : ' (no post-snapshot changes)')
        );
    }

    if (!restoredFromSnapshot || caughtUpChanges) {
        await persistIndexSnapshot(
            state,
            databaseInstanceToken,
            restoredFromSnapshot ? 'catch-up re-persist' : 'initial build persist'
        );
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
    databaseInstanceToken: string,
    reason = 'persist'
): Promise<void> {
    const serializeStartedAt = now();
    const serializedResult = state.index.serialize(false);

    // Handle union return type: string | Promise<Uint8Array> | Uint8Array
    let serializedBody: string;
    if (typeof serializedResult === 'string') {
        serializedBody = serializedResult;
    } else if (serializedResult instanceof Promise) {
        const resolved = await serializedResult;
        serializedBody = new TextDecoder().decode(resolved);
    } else {
        serializedBody = new TextDecoder().decode(serializedResult);
    }

    const persistedBytes = new TextEncoder().encode(serializedBody);
    const serializeDuration = now() - serializeStartedAt;

    const persistStartedAt = now();

    await writeMetaDocument(state, databaseInstanceToken, {
        serialized: Array.from(persistedBytes),
        checkpointId: state.checkpoint?.id,
        checkpointLwt: state.checkpoint?.lwt
    });
    const persistDuration = now() - persistStartedAt;

    logDebug(
        state,
        `${reason}: serialized ${persistedBytes.length.toLocaleString()} bytes in ${serializeDuration.toFixed(2)}ms and persisted in ${persistDuration.toFixed(2)}ms`
    );
    state.changesSinceLastPersist = 0;
    state.firstChangeAt = undefined;
}
