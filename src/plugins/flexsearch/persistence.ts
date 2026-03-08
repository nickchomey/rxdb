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
 * Reads the FlexSearch metadata document from storage.
 */
export async function readMetaDocument(
    state: FlexSearchRuntimeState
): Promise<FlexSearchMetaDocumentData | undefined> {
    if (!state.metaStorage) {
        return undefined;
    }
    const docs = await state.metaStorage.findDocumentsById([state.metaDocumentId], true);
    return docs[0] as FlexSearchMetaDocumentData | undefined;
}

/**
 * Writes a FlexSearch metadata document to storage.
 * Handles revision generation and conflict resolution.
 */
export async function writeMetaDocument(
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
 * Clears the serialized snapshot and checkpoint from metadata.
 * Used when snapshot is corrupted or invalid.
 */
export async function clearMetaDocument(
    state: FlexSearchRuntimeState,
    databaseInstanceToken: string
): Promise<void> {
    await writeMetaDocument(state, databaseInstanceToken, {
        serialized: undefined,
        checkpointId: undefined,
        checkpointLwt: undefined
    });
}

/**
 * Serializes the FlexSearch index using export() API.
 * Returns JSON string of export entries that can be restored with import().
 *
 * NOTE: Document.serialize() doesn't exist - Document uses export/import pattern.
 * Only Index has serialize(). Document exports as [key, data] pairs.
 */
export async function serializeFlexSearchIndex(
    index: FlexSearchRuntimeState['index']
): Promise<string> {
    const entries: Array<[string, unknown]> = [];
    await index.export((key, data) => {
        entries.push([key as string, data]);
    });
    return JSON.stringify(entries);
}

/**
 * Persists the current index snapshot to metadata storage.
 * Updates checkpoint and resets persistence counters.
 */
export async function persistIndexSnapshot(
    state: FlexSearchRuntimeState,
    databaseInstanceToken: string
): Promise<void> {
    const serialized = await serializeFlexSearchIndex(state.index);

    await writeMetaDocument(state, databaseInstanceToken, {
        serialized,
        checkpointId: state.checkpoint?.id,
        checkpointLwt: state.checkpoint?.lwt
    });
    state.changesSinceLastPersist = 0;
    state.firstChangeAt = undefined;
}

/**
 * Returns persistence configuration with defaults.
 */
export function getPersistenceConfig(
    input?: FlexSearchPersistenceConfig
): Required<FlexSearchPersistenceConfig> {
    return {
        minDebounce: input?.minDebounce ?? 1000,
        maxDebounce: input?.maxDebounce ?? 10000,
        adaptive: input?.adaptive ?? true
    };
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

    const config = getPersistenceConfig(persistence);
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
        metaDocument.serialized
    );

    let startCheckpoint: RxStorageDefaultCheckpoint | undefined;
    let restoredFromSnapshot = false;
    if (hasCompatibleSnapshot && metaDocument?.serialized) {
        restoredFromSnapshot = await tryRestoreSnapshot(
            state,
            metaDocument,
            databaseInstanceToken
        );
        if (restoredFromSnapshot && metaDocument.checkpointId && typeof metaDocument.checkpointLwt === 'number') {
            startCheckpoint = {
                id: metaDocument.checkpointId,
                lwt: metaDocument.checkpointLwt
            };
            state.checkpoint = startCheckpoint;
        }
    }

    const caughtUpChanges = await catchUpFromCheckpoint(state, instance, startCheckpoint);
    if (!restoredFromSnapshot || caughtUpChanges) {
        await persistIndexSnapshot(state, databaseInstanceToken);
    }
}

/**
 * Attempts to restore FlexSearch index from export entries.
 * Uses export/import pattern: parse JSON and call index.import() per entry.
 *
 * NOTE: Document doesn't have serialize() - it uses export/import pattern.
 * Returns true if restoration succeeded, false if corrupted/invalid.
 */
export async function tryRestoreSnapshot(
    state: FlexSearchRuntimeState,
    metaDocument: FlexSearchMetaDocument,
    databaseInstanceToken: string
): Promise<boolean> {
    try {
        if (!metaDocument.serialized) {
            return false;
        }
        // Parse the JSON array of [key, data] pairs
        const entries = JSON.parse(metaDocument.serialized) as Array<[string, unknown]>;
        if (!Array.isArray(entries)) {
            throw new Error('Serialized payload is not an array');
        }
        // Import each entry into the index
        entries.forEach(([key, data]) => {
            state.index.import(key, data);
        });
        return true;
    } catch (error) {
        console.error('[FlexSearch] snapshot restore failed, rebuilding index', error);
        await clearMetaDocument(state, databaseInstanceToken);
        return false;
    }
}
