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

    // Extract search term from $fts selector
    const ftsValue = selector?.$fts;
    let searchTerm: string | undefined;
    if (typeof ftsValue === 'string') {
        searchTerm = ftsValue;
    } else if (ftsValue && typeof ftsValue === 'object') {
        const valueRecord = ftsValue as Record<string, unknown>;
        searchTerm = typeof valueRecord.$eq === 'string' ? valueRecord.$eq : undefined;
    }

    if (!searchTerm) {
        return;
    }

    // Extract collection from RxQuery (exists at runtime)
    const collection = (args.rxQuery as any).collection as RxQueryWithCollection['collection'];

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
 * Searches FlexSearch index for matching document IDs.
 * Returns empty array on error to prevent query failure.
 */
function searchFlexIds(
    state: FlexSearchRuntimeState,
    searchTerm: string
): string[] {
    try {
        const searchResult = state.index.search(searchTerm);

        // Normalize FlexSearch results to string array
        if (!Array.isArray(searchResult) || searchResult.length === 0) {
            return [];
        }

        const firstRow = searchResult[0];
        // Check if Document search format: [{ field: string, result: string[] }]
        if (firstRow && typeof firstRow === 'object' && 'result' in firstRow && Array.isArray((firstRow as any).result)) {
            // Document search: flatten all result arrays
            const idSet = new Set<string>();
            searchResult.forEach(row => {
                if (row && typeof row === 'object' && 'result' in row && Array.isArray((row as any).result)) {
                    (row as any).result.forEach((idValue: unknown) => {
                        idSet.add(String(idValue));
                    });
                }
            });
            return Array.from(idSet);
        }

        // Index search: direct ID array
        return searchResult.map(idValue => String(idValue));
    } catch (error) {
        console.error('[FlexSearch] search failed', error);
        return [];
    }
}

/**
 * Syncs the rewritten mango query back to RxQuery.
 * Required for event-reduce to use the rewritten selector.
 */
function syncRewrittenMangoQuery(args: RxPluginPrePrepareQueryArgs): void {
    (args.rxQuery as unknown as { mangoQuery: typeof args.mangoQuery }).mangoQuery = flatClone(args.mangoQuery);
}
