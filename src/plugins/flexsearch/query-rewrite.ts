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
 * - A plain string: used directly as the search query.
 * - An object with $eq: coerced to a plain string (legacy equality form).
 * - A FlexSearchSearchOptions object: passed directly to FlexSearch (use `query` property for the term).
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
 * 1. Extract $fts value from selector (string, legacy $eq, or full options object)
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
    const ftsValue = selector?.$fts;

    // Normalize $fts value to string or FlexSearchSearchOptions:
    //   $fts: "term"           → search("term")
    //   $fts: { $eq: "term" }  → search("term") (legacy RxDB equality form)
    //   $fts: { ... }          → search(options) — options.query carries the search term
    let queryOrOptions: string | FlexSearchSearchOptions;
    if (typeof ftsValue === 'string') {
        queryOrOptions = ftsValue;
    } else if (ftsValue && typeof ftsValue === 'object') {
        const valueRecord = ftsValue as Record<string, unknown>;
        queryOrOptions = typeof valueRecord.$eq === 'string'
            ? valueRecord.$eq
            : ftsValue as FlexSearchSearchOptions;
    } else {
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
    const matchingIds = searchFlexIds(state, queryOrOptions);
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
 * Searches the FlexSearch index and returns a flat deduplicated array of matching document IDs.
 *
 * Accepts either a plain query string or a FlexSearchSearchOptions object.
 * When an options object is passed, FlexSearch's own argument normalization handles it
 * (options.query carries the search term). This mirrors FlexSearch's own search() overloads.
 *
 * Handles all FlexSearch Document search result formats:
 * - Standard:  [{ field, result: [id, ...] }, ...]  (default, enrich: false)
 * - Enriched:  [{ field, result: [{ id, doc }, ...] }, ...]  (enrich: true)
 * - Merged:    [{ id, field[], ... }, ...]  (merge: true)
 * - Flat:      [id, ...]  (Index search, or pluck)
 */
export function searchFlexIds(
    state: FlexSearchRuntimeState,
    queryOrOptions: string | FlexSearchSearchOptions = ''
): string[] {
    try {
        const searchResult = state.index.search(queryOrOptions as any);

        if (!Array.isArray(searchResult) || searchResult.length === 0) {
            return [];
        }

        const idSet = new Set<string>();
        const firstRow = searchResult[0];

        if (typeof firstRow === 'string' || typeof firstRow === 'number') {
            // Flat format: plain ID array (Index search or pluck)
            for (const id of searchResult as unknown[]) {
                if (typeof id === 'string' || typeof id === 'number') {
                    idSet.add(String(id));
                }
            }
        } else if (firstRow && typeof firstRow === 'object') {
            const first = firstRow as Record<string, unknown>;
            if ('result' in first && Array.isArray(first.result)) {
                // Standard Document format: [{ field, result: [id, ...] }, ...]
                // Also handles enrich:true where result items are { id, doc }
                for (const row of searchResult as Array<{ result: unknown[] }>) {
                    for (const item of row.result) {
                        if (typeof item === 'string' || typeof item === 'number') {
                            idSet.add(String(item));
                        } else if (item && typeof item === 'object') {
                            const v = item as Record<string, unknown>;
                            if (v.id !== undefined) idSet.add(String(v.id));
                        }
                    }
                }
            } else if (first.id !== undefined) {
                // Merged format (merge:true): [{ id, field[], doc? }, ...]
                for (const item of searchResult as Array<Record<string, unknown>>) {
                    if (item.id !== undefined) idSet.add(String(item.id));
                }
            }
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
