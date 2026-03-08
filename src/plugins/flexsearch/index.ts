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
import { createFlexSearchIndex, applyEventBulkToIndex } from './indexing.ts';
import { initializeIndexState, schedulePersistence, enqueueStateWork } from './persistence.ts';
import { removeFlexSearchState, setFlexSearchState } from './runtime.ts';
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
            );

            // Subscribe to change stream for real-time sync
            state.changeStreamSubscription = instance.changeStream().subscribe(eventBulk => {
                void enqueueStateWork(state, async () => {
                    await state.initPromise;
                    applyEventBulkToIndex(state, eventBulk);
                    if (persistence) {
                        schedulePersistence(state, persistence, params.databaseInstanceToken);
                    }
                });
            });

            // Wrap storage instance to hook close/remove
            const wrappedInstance = wrapRxStorageInstance(
                params.schema,
                instance,
                doc => doc as any,
                doc => doc as any
            );

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
                    return this.find({
                        selector: {
                            ...selector,
                            $fts: searchTerm as never
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
