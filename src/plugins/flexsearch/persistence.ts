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
    // FIXME: Snapshot restore disabled due to FlexSearch Document type serialization issues.
    // When restore happens, the index becomes inaccessible (search returns empty).
    // This appears to be a limitation with how FlexSearch's serialize() works for Document indexes.
    // For now, we always rebuild from storage truth via catchup on reload.
    if (false && hasCompatibleSnapshot && metaDocument?.serialized) {
        try {
            const compressedBytes = new Uint8Array(metaDocument.serialized);
            let serializedBody: string;
            try {
                // Backward compatible path for previously compressed snapshots.
                serializedBody = await decompress(compressedBytes);
            } catch {
                // Current path stores plain UTF-8 encoded serialize(false) output.
                serializedBody = new TextDecoder().decode(compressedBytes);
            }
            const inject = new Function('doc', serializedBody);
            inject(state.index);
            restoredFromSnapshot = true;
        } catch (error) {
            // Snapshot is optional; if restore fails we rebuild by catch-up/indexing path below.
            console.error('[FlexSearch] snapshot restore failed, rebuilding index', error);
            restoredFromSnapshot = false;
        }

        if (restoredFromSnapshot && metaDocument.checkpointId && typeof metaDocument.checkpointLwt === 'number') {
            startCheckpoint = {
                id: metaDocument.checkpointId,
                lwt: metaDocument.checkpointLwt
            };
            state.checkpoint = startCheckpoint;
        }
    }

    // Always run a full catch-up from storage truth to avoid stale/partial index states.
    // This guarantees correctness even if a persisted snapshot was written before indexing completed.
    const caughtUpChanges = await catchUpFromCheckpoint(state, instance, undefined);

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
