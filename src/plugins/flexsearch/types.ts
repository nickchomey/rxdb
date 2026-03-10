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
    /**
     * Built-in encoder name. Corresponds to the `Charset.*` presets in FlexSearch
     * (e.g. `Charset.LatinAdvanced`). Use `'LatinSoundex'` for phonetic/fuzzy search.
     */
    encoder?: 'Exact' | 'Default' | 'Normalize' | 'LatinBalance' | 'LatinAdvanced' | 'LatinExtra' | 'LatinSoundex' | 'CJK';
    /**
     * Number of index resolution slots. Higher values allow finer relevance scoring.
     * Boost a field's relevance relative to others by giving it a higher resolution.
     * E.g. title: resolution 9, content: resolution 3 makes title matches rank higher.
     */
    resolution?: number;
    context?: boolean | {
        resolution?: number;
        depth?: number;
        bidirectional?: boolean;
    };
    minlength?: number;
    cache?: boolean | number;
    async?: boolean;
};

/**
 * Options passed directly to FlexSearch Document.search().
 * Mirrors FlexSearch's SearchOptions + DocumentSearchOptions.
 *
 * When passed as the first argument to fts(), the `query` property carries the search term.
 * This matches FlexSearch's own `search(options)` overload where options.query is the term.
 */
export type FlexSearchSearchOptions = {
    /** The search query string. Use this when passing a full options object instead of a bare string. */
    query?: string;
    /** Maximum number of results to return. */
    limit?: number;
    /** Number of results to skip (for pagination). */
    offset?: number;
    /** Enable suggestion mode — returns results even for partial/unmatched terms (fuzzy). */
    suggest?: boolean;
    /** Override the resolution (scoring granularity) for this search. */
    resolution?: number;
    /** Enable contextual/phrase search mode. */
    context?: boolean;
    /** Use the search result cache. */
    cache?: boolean;
    /**
     * Restrict search to one or more indexed fields.
     * Field names must match those defined in the schema `fts` config.
     */
    field?: string | string[];
    /**
     * Tag filter: `{ fieldName: tagValue }`.
     * Only returns documents matching the given tag.
     */
    tag?: Record<string, string> | Array<Record<string, string>>;
    /**
     * Include full document data alongside IDs in the raw FlexSearch result.
     * Our ID extraction handles enriched results; the doc data is ignored.
     */
    enrich?: boolean;
    /**
     * Search only the given single field and return a flat result array.
     * Equivalent to `field` but produces a simpler (un-grouped) result shape.
     */
    pluck?: string;
    /**
     * Merge multi-field results into a single array grouped by document ID.
     * Each result item has `{ id, field[] }`. Our extraction reads `id` directly.
     */
    merge?: boolean;
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
