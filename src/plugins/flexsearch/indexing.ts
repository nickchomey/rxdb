/**
 * FlexSearch index operations.
 * Handles index creation, document indexing, change stream processing, and catch-up.
 */

import { Document as FlexSearchDocument, type FieldOptions } from 'flexsearch';
import { getChangedDocumentsSince } from '../../rx-storage-helper.ts';
import { promiseWait } from '../utils/index.ts';
import type {
    EventBulk,
    RxDocumentData,
    RxStorageChangeEvent,
    RxStorageDefaultCheckpoint,
    RxStorageInstance
} from '../../types/index.d.ts';
import type {
    FlexSearchFieldConfig,
    FlexSearchIndexedDocument,
    FlexSearchRuntimeState
} from './types.ts';

const CATCHUP_BATCH_SIZE = 200;

/**
 * Creates a FlexSearch Document index with the given field configurations.
 */
export function createFlexSearchIndex(
    primaryPath: string,
    fields: Array<{ field: string; config: FlexSearchFieldConfig }>,
    defaultOptions: Partial<FlexSearchFieldConfig> = {}
): FlexSearchRuntimeState['index'] {
    const descriptors: Array<FieldOptions<FlexSearchIndexedDocument, false, false>> = fields.map(row => ({
        field: row.field,
        ...defaultOptions,
        ...row.config
    }) as FieldOptions<FlexSearchIndexedDocument, false, false>);

    return new FlexSearchDocument<FlexSearchIndexedDocument>({
        id: primaryPath,
        index: descriptors,
        store: false
    });
}

/**
 * Applies a bulk of change events to the FlexSearch index.
 * Updates checkpoint after processing all events.
 */
export function applyEventBulkToIndex<RxDocType>(
    state: FlexSearchRuntimeState,
    eventBulk: EventBulk<RxStorageChangeEvent<RxDocType>, RxStorageDefaultCheckpoint>
): void {
    eventBulk.events.forEach(event => {
        applyChangeEventToIndex(state, event);
    });

    if (eventBulk.checkpoint) {
        state.checkpoint = {
            id: eventBulk.checkpoint.id,
            lwt: eventBulk.checkpoint.lwt
        };
    }

    state.changesSinceLastPersist =
        (state.changesSinceLastPersist ?? 0) + eventBulk.events.length;
}

/**
 * Converts RxDB document to FlexSearch-compatible document.
 * FlexSearch only needs the fields that are indexed.
 */
function toFlexSearchDocument<RxDocType>(
    documentData: RxDocumentData<RxDocType>
): FlexSearchIndexedDocument {
    return documentData as unknown as FlexSearchIndexedDocument;
}

/**
 * Applies a single change event to the FlexSearch index.
 * Handles INSERT, UPDATE, and DELETE operations.
 */
export function applyChangeEventToIndex<RxDocType>(
    state: FlexSearchRuntimeState,
    event: RxStorageChangeEvent<RxDocType>
): void {
    const documentData = event.documentData;
    const primaryValue = getPrimaryValue(documentData, state.primaryPath);
    if (!primaryValue) {
        return;
    }

    if (event.operation === 'DELETE' || documentData._deleted) {
        state.index.remove(primaryValue);
        return;
    }

    const indexedDocument = toFlexSearchDocument(documentData);
    if (event.operation === 'UPDATE') {
        state.index.update(indexedDocument);
    } else {
        state.index.add(indexedDocument);
    }
}

/**
 * Applies a document to the FlexSearch index.
 * Used during catch-up from checkpoint.
 */
export function applyDocumentToIndex<RxDocType>(
    state: FlexSearchRuntimeState,
    documentData: RxDocumentData<RxDocType>
): void {
    const primaryValue = getPrimaryValue(documentData, state.primaryPath);
    if (!primaryValue) {
        return;
    }

    if (documentData._deleted) {
        state.index.remove(primaryValue);
        return;
    }

    state.index.add(toFlexSearchDocument(documentData));
}


/**
 * Extracts the primary key value from a document.
 * Returns undefined if the primary value is not a string or number.
 */
export function getPrimaryValue<RxDocType>(
    documentData: RxDocumentData<RxDocType>,
    primaryPath: string
): string | undefined {
    const primaryValue = (documentData as Record<string, unknown>)[primaryPath];
    if (typeof primaryValue === 'string') {
        return primaryValue;
    }
    if (typeof primaryValue === 'number') {
        return String(primaryValue);
    }
    return undefined;
}


/**
 * Catches up the index from a checkpoint by applying all changes since that point.
 * Returns true if any changes were applied.
 */
export async function catchUpFromCheckpoint<RxDocType, Internals, InstanceCreationOptions>(
    state: FlexSearchRuntimeState,
    instance: RxStorageInstance<RxDocType, Internals, InstanceCreationOptions, RxStorageDefaultCheckpoint>,
    startCheckpoint?: RxStorageDefaultCheckpoint
): Promise<boolean> {
    let checkpoint = startCheckpoint;
    let hasChanges = false;

    while (true) {
        const changed = await getChangedDocumentsSince(
            instance,
            CATCHUP_BATCH_SIZE,
            checkpoint
        );
        if (changed.documents.length > 0) {
            hasChanges = true;
            changed.documents.forEach(documentData => {
                applyDocumentToIndex(state, documentData);
            });
        }
        checkpoint = changed.checkpoint;
        if (changed.documents.length < CATCHUP_BATCH_SIZE) {
            break;
        }
        // Yield to prevent blocking
        await promiseWait(0);
    }

    if (checkpoint) {
        state.checkpoint = {
            id: checkpoint.id,
            lwt: checkpoint.lwt
        };
    }

    return hasChanges;
}
