import type { Document as FlexSearchDocument } from 'flexsearch';
import type { Subscription } from 'rxjs';
import type { RxStorage, RxStorageDefaultCheckpoint, RxStorageInstance } from '../../types/index.d.ts';

export type FlexSearchIndexValue = string | number | boolean | null;

export type FlexSearchIndexedDocument = Record<
    string,
    FlexSearchIndexValue | FlexSearchIndexValue[]
>;

/**
 * FlexSearch configuration for a single indexed field.
 * This intentionally mirrors the subset of FlexSearch Document field options
 * that we allow inside schema `fts` metadata.
 */
export type FlexSearchFieldConfig = {
    tokenize?: 'strict' | 'forward' | 'reverse' | 'bidirectional' | 'full' | 'tolerant';
    encode?: 'Exact' | 'Default' | 'Normalize' | 'LatinBalance' | 'LatinAdvanced' | 'LatinExtra' | 'LatinSoundex' | 'CJK';
    resolution?: number;
    context?: boolean | {
        resolution?: number;
        depth?: number;
        bidirectional?: boolean;
    };
    minlength?: number;
    cache?: boolean | number;
    async?: boolean;
    /**
     * Field priority/weighting for score boosting in multi-field Document search.
     * Higher values rank matches in this field above matches in lower-priority fields.
     * E.g. set title to 9 and content to 1 to boost title matches 9x.
     */
    priority?: number;
};

/**
 * Options passed to the FlexSearch Document.search() call.
 * Mirrors the FlexSearch DocumentSearchOptions type for the fields we expose.
 */
export type FlexSearchSearchOptions = {
    /** Maximum number of results to return. */
    limit?: number;
    /** Enable fuzzy/suggestion mode - returns results even with partial/typo mismatches. */
    suggest?: boolean;
    /** Restrict the search to specific indexed fields. */
    field?: string | string[];
};

export type FlexSearchPersistenceConfig = {
    minDebounce?: number;
    maxDebounce?: number;
    adaptive?: boolean;
};

export type FlexSearchWrapperConfig = {
    storage: RxStorage<unknown, unknown>;
    persistence?: FlexSearchPersistenceConfig;
    defaultIndexOptions?: Partial<FlexSearchFieldConfig>;
};

export type FlexSearchMetaDocument = {
    id: string;
    serialized?: number[];
    checkpointId?: string;
    checkpointLwt?: number;
    version: number;
    timestamp: number;
    schemaHash?: string;
};

export type FlexSearchMetaDocumentData = FlexSearchMetaDocument & {
    _attachments: Record<PropertyKey, never>;
    _deleted: boolean;
    _meta: {
        lwt: number;
    };
    _rev: string;
};

export type FlexSearchMetaStorageInstance = RxStorageInstance<
    FlexSearchMetaDocument,
    unknown,
    unknown,
    RxStorageDefaultCheckpoint
>;

export type FlexSearchRuntimeState = {
    index: FlexSearchDocument<FlexSearchIndexedDocument, false, false>;
    primaryPath: string;
    indexedFields: string[];
    changeStreamSubscription?: Subscription;
    initPromise?: Promise<void>;
    checkpoint?: RxStorageDefaultCheckpoint;
    writeQueue?: Promise<void>;
    metaStorage?: FlexSearchMetaStorageInstance;
    persistenceTimer?: ReturnType<typeof setTimeout>;
    changesSinceLastPersist?: number;
    firstChangeAt?: number;
    dynamicDebounceMs?: number;
    schemaHash: string;
    metaDocumentId: string;
};

export const FLEXSEARCH_META_VERSION = 1;
