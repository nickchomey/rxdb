---
title: Dump File - Binary Export & Import
slug: dump-file.html
description: Export and import entire RxDB databases or specific collections as a single gzip-compressed binary file, including blob attachments.
---

# Dump File

The `dump-file` plugin provides streaming binary export and import for RxDB databases. It produces a single **gzip-compressed** file containing all documents and their blob attachments. This is useful for creating full backups, migrating data between devices, or transferring a database snapshot.

Unlike the JSON-based `json-dump` plugin, the dump-file plugin:
- Uses a compact **binary format** instead of JSON
- Includes **blob attachments** by default
- Produces a single **gzip-compressed** file
- Supports **streaming import** via `DecompressionStream` for memory efficiency
- Works across **all JavaScript runtimes** (Browser, Node.js, Deno, Bun)

## Setup

```ts
import { addRxPlugin } from 'rxdb';
import { RxDBDumpFilePlugin } from 'rxdb/plugins/dump-file';
addRxPlugin(RxDBDumpFilePlugin);
```

## db.export()

Export the entire database (or selected collections) to a compressed `Blob`.

```ts
// Export all collections
const blob = await myDatabase.export();

// Export only specific collections
const blob = await myDatabase.export({
    collections: ['heroes', 'villains']
});

// Export without attachments
const blob = await myDatabase.export({
    attachments: false
});
```

### Options

| Option | Type | Default | Description |
|---|---|---|---|
| `collections` | `string[]` | all | Which collections to include |
| `attachments` | `boolean` | `true` | Whether to include attachment data |


## db.import()

Import a dump file into an existing database. Collections that exist in the dump but not in the database will be auto-created using the schemas stored in the export header.

```ts
// Import into an existing database
await myDatabase.import(blob);

// Import with options
await myDatabase.import(blob, {
    overwriteOnConflict: true,  // default: true
    batchSize: 200
});
```

### Options

| Option | Type | Default | Description |
|---|---|---|---|
| `overwriteOnConflict` | `boolean` | `true` | Whether to overwrite existing documents on conflict |
| `batchSize` | `number` | `100` | Number of documents per bulk write batch |


## importDatabase() (standalone)

Create a new database from a dump file without having an existing database instance. You must provide the `storage` option so RxDB knows which storage engine to use.

```ts
import { importDatabase } from 'rxdb/plugins/dump-file';

const db = await importDatabase(blob, {
    storage: myRxStorage
});

// db is now a fully initialized RxDatabase with all collections and data
```


## Saving to a file

The plugin exports a `saveToFile()` helper that works across environments:

- **Node.js / Bun**: writes via `node:fs/promises`
- **Deno**: writes via `Deno.writeFile`
- **Browser**: uses the [File System Access API](https://developer.mozilla.org/en-US/docs/Web/API/File_System_Access_API) with an anchor-download fallback

```ts
import { saveToFile } from 'rxdb/plugins/dump-file';

const blob = await myDatabase.export();
await saveToFile(blob, 'my-database-backup.rxdb');
```


## Binary format

The dump file uses a custom binary format inside a gzip stream:

```
[4B magic "RXDB"] [4B version uint32]
[4B+NB meta JSON] — { dbName, collections: [{ name, schema, schemaHash }] }
Per collection:
  [4B+NB collectionName] [4B docCount uint32]
  Per document:
    [4B+NB doc JSON (attachment stubs only)]
    [2B attachmentCount uint16]
    Per attachment:
      [2B+NB attachmentId]
      [4B+NB raw blob bytes]
[4B 0x00000000 end marker]
```

The full collection schemas are stored in the header, so import can auto-create collections without requiring them to be pre-configured.
