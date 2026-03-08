/**
 * FlexSearch query rewriting.
 * Intercepts RxDB queries and rewrites $fts selectors to primaryKey $in filters.
 */

import { flatClone } from '../utils/index.ts';
import type { MangoQuerySelector } from '../../types/index.d.ts';
import type { RxPluginPrePrepareQueryArgs } from '../../types/rx-plugin.d.ts';
import type { FlexSearchRuntimeState } from './types.ts';
import { getFlexSearchState } from './runtime.ts';

/**
 * FlexSearch selector type with $fts operator.
 */
type FlexSearchSelector = MangoQuerySelector<Record<string, unknown>> & {
    $fts?: unknown;
};

/**
 * RxQuery with collection property (runtime only, not in declared type).
 */
type RxQueryWithCollection = {
    collection: {
        database: { name: string };
        name: string;
    };
};


/**
 * Type guard for FlexSearch Document search results.
 * Document search returns: [{ field: string, result: string[] }]
 */
function isFlexSearchDocumentResult(value: unknown): value is { field: string; result: unknown[] } {
    if (!value || typeof value !== 'object') {
        return false;
    }
    const valueRecord = value as Record<string, unknown>;
    return Array.isArray(valueRecord.result);
}

/**
 * Normalizes FlexSearch search results to string array.
 * Handles both Index search (returns string[]) and Document search (returns [{ result: string[] }]).
 */
function normalizeSearchResultIds(searchResult: unknown): string[] {
    if (!Array.isArray(searchResult)) {
        return [];
    }
    if (searchResult.length === 0) {
        return [];
    }

    const firstRow = searchResult[0];
    if (isFlexSearchDocumentResult(firstRow)) {
        // Document search: flatten all result arrays
        const idSet = new Set<string>();
        searchResult.forEach(row => {
            if (!isFlexSearchDocumentResult(row)) {
                return;
            }
            row.result.forEach(idValue => {
                idSet.add(String(idValue));
            });
        });
        return Array.from(idSet);
    }

    // Index search: direct ID array
    return searchResult.map(idValue => String(idValue));
}



/**
 * Extracts search term from $fts selector value.
 * Supports both string and { $eq: string } forms.
 */
export function getFlexSearchSearchTerm(value: unknown): string | undefined {
    if (typeof value === 'string') {
        return value;
    }
    if (!value || typeof value !== 'object') {
        return undefined;
    }

    const valueRecord = value as Record<string, unknown>;
    return typeof valueRecord.$eq === 'string' ? valueRecord.$eq : undefined;
}

/**
 * Searches FlexSearch index for matching document IDs.
 * Returns empty array on error to prevent query failure.
 */
export function searchFlexIds(
    state: FlexSearchRuntimeState,
    searchTerm: string
): string[] {
    try {
        return normalizeSearchResultIds(state.index.search(searchTerm));
    } catch (error) {
        console.error('[FlexSearch] search failed', error);
        return [];
    }
}

/**
 * Rewrites RxDB query selector to replace $fts with primaryKey $in filter.
 * This is the core query integration point for the plugin.
 *
 * Flow:
 * 1. Extract $fts search term from selector
 * 2. Search FlexSearch index for matching IDs
 * 3. Remove $fts from selector
 * 4. Inject primaryKey: { $in: [...matchingIds] }
 * 5. Sync rewritten query back to RxQuery for event-reduce
 */
export function rewriteFtsSelector(args: RxPluginPrePrepareQueryArgs): void {
    const selector = args.mangoQuery.selector as FlexSearchSelector;
    const searchTerm = getFlexSearchSearchTerm(selector?.$fts);
    if (!searchTerm) {
        return;
    }

    const collection = getRxQueryCollection(args);

    const state = getFlexSearchState(
        collection.database.name,
        collection.name
    );
    if (!state) {
        // Collection doesn't have FTS enabled, remove $fts and pass through
        delete selector.$fts;
        return;
    }

    const matchingIds = searchFlexIds(state, searchTerm);
    const baseSelector = flatClone(selector);
    delete baseSelector.$fts;

    const idSelector: MangoQuerySelector<Record<string, unknown>> = {
        [state.primaryPath]: {
            $in: matchingIds
        }
    };

    // Handle case where selector already has primary key constraint
    if (Object.prototype.hasOwnProperty.call(baseSelector, state.primaryPath)) {
        const primarySelector = baseSelector[state.primaryPath];
        delete baseSelector[state.primaryPath];
        args.mangoQuery.selector = {
            $and: [
                baseSelector,
                {
                    [state.primaryPath]: primarySelector
                },
                idSelector
            ]
        } as MangoQuerySelector<Record<string, unknown>>;
        syncRewrittenMangoQuery(args);
        return;
    }

    args.mangoQuery.selector = {
        ...baseSelector,
        ...idSelector
    };
    syncRewrittenMangoQuery(args);
}

/**
 * Syncs the rewritten mango query back to RxQuery.
 * Required for event-reduce to use the rewritten selector.
 */
export function syncRewrittenMangoQuery(args: RxPluginPrePrepareQueryArgs): void {
    (args.rxQuery as unknown as { mangoQuery: typeof args.mangoQuery }).mangoQuery = flatClone(args.mangoQuery);
}

/**
 * Extracts the collection from RxQuery.
 * The collection property exists at runtime but isn't in the type declaration.
 */
export function getRxQueryCollection(args: RxPluginPrePrepareQueryArgs): RxQueryWithCollection['collection'] {
    return (args.rxQuery as any).collection as RxQueryWithCollection['collection'];
}
