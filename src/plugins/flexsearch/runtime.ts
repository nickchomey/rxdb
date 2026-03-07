/**
 * FlexSearch runtime state management.
 * Handles lifecycle, state storage, and cleanup for per-collection indexes.
 *
 * Runtime state is stored in-memory keyed by "{databaseName}::{collectionName}".
 * Each FTS-enabled collection gets one FlexSearchRuntimeState instance.
 */

import type { FlexSearchRuntimeState } from './types.ts';

/**
 * Global in-memory storage for FlexSearch runtime states.
 * One entry per FTS-enabled collection.
 */
const RUNTIME_STATES = new Map<string, FlexSearchRuntimeState>();

/**
 * Retrieves the FlexSearch runtime state for a collection.
 * Returns undefined if the collection doesn't have FTS enabled.
 */
export function getFlexSearchState(
    databaseName: string,
    collectionName: string
): FlexSearchRuntimeState | undefined {
    return RUNTIME_STATES.get(getRuntimeStateKey(databaseName, collectionName));
}

/**
 * Stores a FlexSearch runtime state for a collection.
 */
export function setFlexSearchState(
    databaseName: string,
    collectionName: string,
    state: FlexSearchRuntimeState
): void {
    RUNTIME_STATES.set(getRuntimeStateKey(databaseName, collectionName), state);
}

/**
 * Removes and cleans up FlexSearch runtime state.
 * Unsubscribes from change streams, clears timers, and closes storage.
 */
export async function removeFlexSearchState(
    databaseName: string,
    collectionName: string
): Promise<void> {
    const stateKey = getRuntimeStateKey(databaseName, collectionName);
    const state = RUNTIME_STATES.get(stateKey);
    if (!state) {
        return;
    }

    // Unsubscribe from change stream
    state.changeStreamSubscription?.unsubscribe();

    // Clear persistence timer
    if (state.persistenceTimer) {
        clearTimeout(state.persistenceTimer);
    }

    // Wait for any pending work
    if (state.writeQueue) {
        try {
            await state.writeQueue;
        } catch {
            // Ignore errors during cleanup
        }
    }

    // Close meta storage
    if (state.metaStorage) {
        try {
            await state.metaStorage.close();
        } catch {
            // Ignore errors during cleanup
        }
    }

    RUNTIME_STATES.delete(stateKey);
}

/**
 * Generates the runtime state key for a collection.
 * Format: "{databaseName}::{collectionName}"
 */
function getRuntimeStateKey(databaseName: string, collectionName: string): string {
    return `${databaseName}::${collectionName}`;
}