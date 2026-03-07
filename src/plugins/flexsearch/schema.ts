/**
 * Schema utilities for FlexSearch plugin.
 * Handles FTS config extraction, schema keyword stripping, and meta schema generation.
 */

import { fillWithDefaultSettings, getPrimaryFieldOfPrimaryKey } from '../../rx-schema-helper.ts';
import { flatClone } from '../utils/index.ts';
import type { RxDocumentData, RxJsonSchema } from '../../types/index.d.ts';
import type { FlexSearchFieldConfig, FlexSearchMetaDocument } from './types.ts';
import { FLEXSEARCH_META_VERSION } from './types.ts';

/**
 * Extracts FlexSearch configuration from schema field definitions.
 * Returns null if no fields have `fts` config.
 */
export function extractFlexSearchConfig<RxDocType>(schema: RxJsonSchema<RxDocType>): {
    fields: Array<{ field: string; config: FlexSearchFieldConfig }>;
    primaryPath: string;
} | null {
    const primaryPath = getPrimaryFieldOfPrimaryKey(schema.primaryKey);
    const fields: Array<{ field: string; config: FlexSearchFieldConfig }> = [];

    Object.entries(schema.properties).forEach(([fieldName, fieldDefinition]) => {
        // Inline extraction of fts config from field definition
        if (!fieldDefinition || typeof fieldDefinition !== 'object') {
            return;
        }
        const fieldRecord = fieldDefinition as Record<string, unknown>;
        const ftsConfig = fieldRecord.fts;
        if (!ftsConfig || typeof ftsConfig !== 'object') {
            return;
        }
        fields.push({
            field: fieldName,
            config: ftsConfig as FlexSearchFieldConfig
        });
    });

    return fields.length === 0 ? null : { fields, primaryPath };
}

/**
 * Strips `fts` keywords from schema to prevent AJV validation errors.
 * The underlying storage doesn't know about FlexSearch config.
 */
export function stripFlexSearchSchemaKeywords<RxDocType>(
    schema: RxJsonSchema<RxDocType>
): RxJsonSchema<RxDocType> {
    const childSchema = flatClone(schema);
    childSchema.properties = flatClone(childSchema.properties);

    Object.entries(childSchema.properties).forEach(([fieldName, fieldDefinition]) => {
        if (!fieldDefinition || typeof fieldDefinition !== 'object' || !('fts' in fieldDefinition)) {
            return;
        }
        const clonedDefinition = flatClone(fieldDefinition as Record<string, unknown>);
        delete clonedDefinition.fts;
        (childSchema.properties as Record<string, unknown>)[fieldName] = clonedDefinition;
    });

    return childSchema;
}

/**
 * Generates the schema for FlexSearch metadata storage.
 * Stores serialized index snapshots and checkpoints.
 */
export function getFlexSearchMetaSchema(): RxJsonSchema<RxDocumentData<FlexSearchMetaDocument>> {
    return fillWithDefaultSettings<FlexSearchMetaDocument>({
        title: 'RxDBFlexSearchMeta',
        primaryKey: 'id',
        type: 'object',
        version: 0,
        additionalProperties: false,
        properties: {
            id: {
                type: 'string',
                maxLength: 120
            },
            serialized: {
                type: 'string'
            },
            checkpointId: {
                type: 'string'
            },
            checkpointLwt: {
                type: 'number',
                minimum: 0,
                maximum: 9999999999999999,
                multipleOf: 0.01
            },
            version: {
                type: 'number',
                minimum: 1,
                maximum: 999999,
                multipleOf: 1
            },
            timestamp: {
                type: 'number',
                minimum: 0,
                maximum: 9999999999999999,
                multipleOf: 0.01
            },
            schemaHash: {
                type: 'string'
            }
        },
        indexes: [
            ['timestamp']
        ],
        required: ['id', 'version', 'timestamp']
    });
}

/**
 * Computes a hash of the schema to detect breaking changes.
 * Used to invalidate persisted snapshots when schema changes.
 */
export function computeSchemaHash<RxDocType>(schema: RxJsonSchema<RxDocType>): string {
    return JSON.stringify({
        version: schema.version,
        primaryKey: schema.primaryKey,
        indexes: schema.indexes,
        properties: schema.properties
    });
}
