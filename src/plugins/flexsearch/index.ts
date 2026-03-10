/**
 * RxDB FlexSearch Plugin
 *
 * Full-text search integration using FlexSearch as a storage wrapper.
 * Automatically indexes collections with `fts` config on schema fields.
 *
 * Architecture:
 * - Storage wrapper intercepts createStorageInstance()
 * - Per-collection FlexSearch indexes stored in runtime state
 * - Syncs via changeStream subscription (post-success writes only)
 * - Persists snapshots using serialize() API for fast restoration
 * - Query rewriting via prePrepareQuery hook ($fts -> primaryKey $in)
 *
 * Modules:
 * - schema.ts: FTS config extraction, schema keyword stripping
 * - helpers.ts: Utilities (key generation, ID normalization)
 * - indexing.ts: Index creation and change application
 * - persistence.ts: Serialize/restore with serialize() API
 * - runtime.ts: State management and lifecycle
 * - query-rewrite.ts: Query hook and selector rewriting
 */

import { wrapRxStorageInstance } from '../../plugin-helpers.ts';
import type {
    RxPlugin,
    RxStorage,
    RxStorageInstanceCreationParams
} from '../../types/index.d.ts';
import type { RxPluginPrePrepareQueryArgs } from '../../types/rx-plugin.d.ts';
import type { FlexSearchRuntimeState, FlexSearchWrapperConfig } from './types.ts';

// Module imports
import { computeSchemaHash, extractFlexSearchConfig, getFlexSearchMetaSchema, stripFlexSearchSchemaKeywords } from './schema.ts';
import { createFlexSearchIndex, catchUpFromCheckpoint } from './indexing.ts';
import { initializeIndexState, schedulePersistence, enqueueStateWork } from './persistence.ts';
import { removeFlexSearchState, setFlexSearchState, getFlexSearchState } from './runtime.ts';
import { rewriteFtsSelector } from './query-rewrite.ts';

// Constants
const FLEXSEARCH_META_COLLECTION_PREFIX = '_rxdb_flexsearch_meta_';
const FLEXSEARCH_META_DOC_ID = 'index-state';

/**
 * Type alias for wrapped storage instances.
 */
type WrappedStorageInstance<RxDocType, Internals, InstanceCreationOptions> = Awaited<
    ReturnType<RxStorage<Internals, InstanceCreationOptions>['createStorageInstance']>
>;

/**
 * Collection protocol extension for .fts() helper method.
 */
type FlexSearchCollectionPrototype = {
    fts?: (searchTerm: string, selector?: Record<string, unknown>) => unknown;
    find: (query: { selector: Record<string, unknown> }) => unknown;
};

/**
 * Creates a FlexSearch storage wrapper.
 * Collections with `fts`-configured fields get automatic FTS indexing.
 * Non-FTS collections pass through unchanged.
 */
export function wrappedFlexSearchStorage<Internals, InstanceCreationOptions>(
    config: FlexSearchWrapperConfig
): RxStorage<Internals, InstanceCreationOptions> {
    const { storage, persistence, defaultIndexOptions } = config;
    const wrappedStorageName = storage.name.startsWith('validate-')
        ? storage.name.replace('validate-', 'validate-flexsearch-')
        : `flexsearch-${storage.name}`;

    return Object.assign({}, storage, {
        name: wrappedStorageName,
        async createStorageInstance<RxDocType>(
            params: RxStorageInstanceCreationParams<RxDocType, InstanceCreationOptions>
        ): Promise<WrappedStorageInstance<RxDocType, Internals, InstanceCreationOptions>> {
            // Extract FTS config from schema
            const ftsConfig = extractFlexSearchConfig(params.schema);
            if (!ftsConfig) {
                // No FTS fields - pass through unchanged
                return await storage.createStorageInstance(params as any) as WrappedStorageInstance<
                    RxDocType,
                    Internals,
                    InstanceCreationOptions
                >;
            }

            // Clean up any existing state
            await removeFlexSearchState(params.databaseName, params.collectionName);

            // Strip FTS keywords from schema before passing to underlying storage
            const childSchema = stripFlexSearchSchemaKeywords(params.schema);

            const instance = await storage.createStorageInstance(
                Object.assign({}, params, {
                    schema: childSchema as typeof params.schema
                }) as typeof params
            ) as WrappedStorageInstance<RxDocType, Internals, InstanceCreationOptions>;

            // Create FlexSearch index
            const flexIndex = createFlexSearchIndex(
                ftsConfig.primaryPath,
                ftsConfig.fields,
                defaultIndexOptions
            );

            // Create meta storage for snapshots
            const metaCollectionName = FLEXSEARCH_META_COLLECTION_PREFIX + params.collectionName;
            const metaStorage = await storage.createStorageInstance({
                databaseInstanceToken: params.databaseInstanceToken,
                databaseName: params.databaseName,
                collectionName: metaCollectionName,
                schema: getFlexSearchMetaSchema(),
                options: params.options,
                multiInstance: params.multiInstance,
                password: params.password,
                devMode: params.devMode
            }) as FlexSearchRuntimeState['metaStorage'];

            // Initialize runtime state
            const state: FlexSearchRuntimeState = {
                index: flexIndex,
                primaryPath: ftsConfig.primaryPath,
                indexedFields: ftsConfig.fields.map(f => f.field),
                metaStorage,
                schemaHash: computeSchemaHash(params.schema),
                metaDocumentId: FLEXSEARCH_META_DOC_ID
            };

            setFlexSearchState(params.databaseName, params.collectionName, state);

            // Initialize from persisted snapshot or rebuild
            state.initPromise = initializeIndexState(
                state,
                instance,
                params.databaseInstanceToken
            ).finally(() => {
                state.initPromise = undefined;
            });

            // Keep index in sync after writes by catching up from the last checkpoint.
            const originalInstanceBulkWrite = instance.bulkWrite.bind(instance);
            instance.bulkWrite = async (documentWrites: any[], context: string) => {
                const writeResult = await originalInstanceBulkWrite(documentWrites, context);
                void enqueueStateWork(state, async () => {
                    const hadChanges = await catchUpFromCheckpoint(state, instance, state.checkpoint);
                    if (hadChanges && persistence) {
                        schedulePersistence(state, persistence, params.databaseInstanceToken);
                    }
                });
                return writeResult;
            };

            // Wrap storage instance to hook close/remove
            const wrappedInstance = wrapRxStorageInstance(
                params.schema,
                instance,
                doc => doc as any,
                doc => doc as any
            );

            // Attach FlexSearch state to wrapped instance so it's accessible from collection
            (wrappedInstance as any)._flexSearchState = state;

            // Hook cleanup into close/remove
            const originalClose = wrappedInstance.close.bind(wrappedInstance);
            wrappedInstance.close = async () => {
                await removeFlexSearchState(params.databaseName, params.collectionName);
                return await originalClose();
            };

            const originalRemove = wrappedInstance.remove.bind(wrappedInstance);
            wrappedInstance.remove = async () => {
                await removeFlexSearchState(params.databaseName, params.collectionName);
                return await originalRemove();
            };

            return wrappedInstance as WrappedStorageInstance<RxDocType, Internals, InstanceCreationOptions>;
        }
    });
}

/**
 * RxDB plugin export.
 * Registers query hook and collection prototype extensions.
 */
export const RxDBFlexSearchPlugin: RxPlugin = {
    name: 'flexsearch',
    rxdb: true,
    prototypes: {
        RxCollection: (proto: object) => {
            const collectionProto = proto as FlexSearchCollectionPrototype;
            if (!collectionProto.fts) {
                collectionProto.fts = function (searchTerm: string, selector?: Record<string, unknown>) {
                    // Get the FlexSearch state
                    const database = (this as any).database;
                    const collectionName = (this as any).name;
                    const flexSearchState = getFlexSearchState(database.name, collectionName);

                    if (!flexSearchState) {
                        // Fallback: no FTS configured, use normal find with $fts selector
                        return this.find({
                            selector: {
                                ...(selector || {}),
                                $fts: searchTerm as never
                            }
                        });
                    }

                    // Search FlexSearch index directly to get matching IDs
                    const matchingIds = ((): string[] => {
                        try {
                            const searchResult = flexSearchState.index.search(searchTerm);
                            if (!Array.isArray(searchResult) || searchResult.length === 0) {
                                return [];
                            }

                            const firstRow = searchResult[0];
                            let idSet = new Set<string>();

                            // Check if Document search format: [{ field: string, result: string[] }]
                            if (firstRow && typeof firstRow === 'object' && 'result' in firstRow && Array.isArray((firstRow as any).result)) {
                                // Document search: flatten all result arrays
                                searchResult.forEach(row => {
                                    if (row && typeof row === 'object' && 'result' in row && Array.isArray((row as any).result)) {
                                        (row as any).result.forEach((idValue: unknown) => {
                                            if (typeof idValue === 'string' || typeof idValue === 'number') {
                                                idSet.add(String(idValue));
                                                return;
                                            }
                                            if (idValue && typeof idValue === 'object') {
                                                const valueObject = idValue as Record<string, unknown>;
                                                if (typeof valueObject.id === 'string' || typeof valueObject.id === 'number') {
                                                    idSet.add(String(valueObject.id));
                                                    return;
                                                }
                                                const docObject = valueObject.doc as Record<string, unknown> | undefined;
                                                if (docObject && (typeof docObject.id === 'string' || typeof docObject.id === 'number')) {
                                                    idSet.add(String(docObject.id));
                                                }
                                            }
                                        });
                                    }
                                });
                            } else {
                                // Index search: direct ID array
                                idSet = new Set(searchResult.map(idValue => String(idValue)));
                            }

                            return Array.from(idSet);
                        } catch (error) {
                            console.error('[FlexSearch] search failed', error);
                            return [];
                        }
                    })();

                    // RxDB now automatically optimizes $in on primaryPath to use findByIds-like mechanism, before using QueryMatcher on the remainder of the query
                    const primaryPath = this.schema.primaryPath as string;
                    return this.find({
                        selector: {
                            ...(selector || {}),
                            [primaryPath]: {
                                $in: matchingIds
                            }
                        }
                    });
                };
            }
        }
    },
    overwritable: {},
    hooks: {
        prePrepareQuery: {
            after: (args: RxPluginPrePrepareQueryArgs) => {
                rewriteFtsSelector(args);
            }
        }
    }
};

/**
 * Re-export runtime state utilities for testing and debugging.
 */
export { getFlexSearchState } from './runtime.ts';

/**
 * Export all types.
 */
export * from './types.ts';
