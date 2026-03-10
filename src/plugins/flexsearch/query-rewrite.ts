/**
 * FlexSearch query rewriting.
 * Intercepts RxDB queries and rewrites $fts selectors to primaryKey $in filters.
 */

import { flatClone } from '../utils/index.ts';
import type { MangoQuerySelector } from '../../types/index.d.ts';
import type { RxPluginPrePrepareQueryArgs } from '../../types/rx-plugin.d.ts';
import type { FlexSearchRuntimeState, FlexSearchSearchOptions } from './types.ts';
import { getFlexSearchState } from './runtime.ts';

/**
 * FlexSearch selector type with $fts operator.
 * The $fts value can be:
 * - A plain string: used as the search query.
 * - An object with $eq: used as a strict equality query match.
 * - A FlexSearchSearchOptions object (with optional `query` field): passed directly to FlexSearch.
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
    // Safety check: ensure args exists and has mangoQuery
    if (!args || !args.mangoQuery || !args.mangoQuery.selector) {
        return;
    }

    const rewriteStart = performance.now();
    const selector = args.mangoQuery.selector as FlexSearchSelector;

    // Extract search term and options from $fts selector.
    // Supported forms:
    //   $fts: "searchTerm"                  -> plain query string
    //   $fts: { $eq: "searchTerm" }         -> legacy equality form
    //   $fts: { query: "...", limit: 5, suggest: true, field: [...] }  -> full options object
    const ftsValue = selector?.$fts;
    let searchQuery: string | undefined;
    let searchOptions: FlexSearchSearchOptions = {};

    if (typeof ftsValue === 'string') {
        searchQuery = ftsValue;
    } else if (ftsValue && typeof ftsValue === 'object') {
        const valueRecord = ftsValue as Record<string, unknown>;
        if (typeof valueRecord.$eq === 'string') {
            // Legacy { $eq: "term" } form
            searchQuery = valueRecord.$eq;
        } else {
            // Full options object: { query?, limit?, suggest?, field? }
            if (typeof valueRecord.query === 'string') {
                searchQuery = valueRecord.query;
            }
            if (typeof valueRecord.limit === 'number') {
                searchOptions.limit = valueRecord.limit;
            }
            if (typeof valueRecord.suggest === 'boolean') {
                searchOptions.suggest = valueRecord.suggest;
            }
            if (valueRecord.field !== undefined) {
                searchOptions.field = valueRecord.field as string | string[];
            }
        }
    }

    // Require at least a query string or a non-empty options object to proceed
    if (searchQuery === undefined && Object.keys(searchOptions).length === 0) {
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

    const searchStart = performance.now();
    const matchingIds = searchFlexIds(state, searchQuery, searchOptions);
    const searchDuration = performance.now() - searchStart;
    console.log(`[FlexSearch] Query hook search: ${matchingIds.length} IDs found in ${searchDuration.toFixed(2)}ms`);
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

    const totalDuration = performance.now() - rewriteStart;
    if (totalDuration > 2) {
        console.log(`[FlexSearch] Query rewrite complete in ${totalDuration.toFixed(2)}ms`);
    }
}


/**
 * Searches FlexSearch index for matching document IDs.
 * Returns empty array on error to prevent query failure.
 */
function searchFlexIds(
    state: FlexSearchRuntimeState,
    query?: string,
    options: FlexSearchSearchOptions = {}
): string[] {
    try {
        // Time the FlexSearch search operation
        const searchStartTime = performance.now();
        const searchResult = query !== undefined
            ? state.index.search(query, options as any)
            : state.index.search(options as any);
        const searchDuration = performance.now() - searchStartTime;

        // Normalize FlexSearch results to string array
        if (!Array.isArray(searchResult) || searchResult.length === 0) {
            if (searchDuration > 10) {
                console.debug(`[FlexSearch] No results found in ${searchDuration.toFixed(2)}ms`);
            }
            return [];
        }

        const extractStartTime = performance.now();
        const firstRow = searchResult[0];
        const idSet = new Set<string>();

        // Check if Document search format: [{ field: string, result: string[] }]
        if (firstRow && typeof firstRow === 'object' && 'result' in firstRow && Array.isArray((firstRow as any).result)) {
            // Document search: flatten all result arrays
            for (const row of searchResult as Array<{ field?: string; result: unknown[] }>) {
                for (const idValue of row.result) {
                    if (typeof idValue === 'string' || typeof idValue === 'number') {
                        idSet.add(String(idValue));
                    } else if (idValue && typeof idValue === 'object') {
                        const valueObject = idValue as Record<string, unknown>;
                        if (typeof valueObject.id === 'string' || typeof valueObject.id === 'number') {
                            idSet.add(String(valueObject.id));
                        } else {
                            const docObject = valueObject.doc as Record<string, unknown> | undefined;
                            if (docObject && (typeof docObject.id === 'string' || typeof docObject.id === 'number')) {
                                idSet.add(String(docObject.id));
                            }
                        }
                    }
                }
            }
        } else {
            // Index search: direct ID array
            for (const idValue of searchResult as unknown[]) {
                idSet.add(String(idValue));
            }
        }

        const extractDuration = performance.now() - extractStartTime;
        const resultCount = idSet.size;

        if (searchDuration > 5 || extractDuration > 5) {
            console.debug(`[FlexSearch] Search completed: ${searchDuration.toFixed(2)}ms search + ${extractDuration.toFixed(2)}ms extraction = ${(searchDuration + extractDuration).toFixed(2)}ms total, ${resultCount} results`);
        }

        return Array.from(idSet);
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
