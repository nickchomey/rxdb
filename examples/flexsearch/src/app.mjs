import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  addRxPlugin,
  createRxDatabase,
  randomToken
} from '../../../plugins/core/index.mjs';
import { getRxStorageMemory } from '../../../plugins/storage-memory/index.mjs';
import {
  RxDBFlexSearchPlugin,
  wrappedFlexSearchStorage
} from '../../../plugins/flexsearch/index.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const transformedPath = path.resolve(__dirname, '../files/items.transformed.json');

addRxPlugin(RxDBFlexSearchPlugin);

async function loadItems(limit = 5000) {
  const raw = await fs.readFile(transformedPath, 'utf8');
  const docs = JSON.parse(raw);
  return docs.slice(0, limit);
}

function printResults(query, docs) {
  console.log(`\nQuery: ${query}`);
  console.log(`Matches: ${docs.length}`);

  docs.forEach((doc, index) => {
    const preview = doc.content.replace(/\s+/g, ' ').slice(0, 140);
    console.log(`\n${index + 1}. ${doc.title}`);
    console.log(`   ${preview}${doc.content.length > 140 ? '...' : ''}`);
  });
}

async function run() {
  const limit = Number(process.env.FLEXSEARCH_EXAMPLE_LIMIT ?? 5000);
  const query = process.argv.slice(2).join(' ').trim() || 'city in germany';

  let items;
  try {
    items = await loadItems(limit);
  } catch (error) {
    console.error('Could not read transformed dataset. Run `npm run prepare-data` first.');
    throw error;
  }

  const db = await createRxDatabase({
    name: 'flexsearch-example-' + randomToken(6),
    storage: wrappedFlexSearchStorage({
      storage: getRxStorageMemory(),
      persistence: {
        minDebounce: 100,
        maxDebounce: 1000,
        adaptive: true
      }
    }),
    multiInstance: false,
    eventReduce: true
  });

  const collections = await db.addCollections({
    items: {
      schema: {
        version: 0,
        primaryKey: 'id',
        type: 'object',
        properties: {
          id: {
            type: 'string',
            maxLength: 120
          },
          title: {
            type: 'string',
            fts: {
              tokenize: 'forward',
              resolution: 9
            }
          },
          content: {
            type: 'string',
            fts: {
              tokenize: 'forward',
              resolution: 9
            }
          }
        },
        required: ['id', 'title', 'content']
      }
    }
  });

  await collections.items.bulkInsert(items);

  const probeTerm = items[0]?.title?.split(/\s+/)[0] || '';
  if (probeTerm) {
    for (let i = 0; i < 50; i++) {
      const readyProbe = await collections.items.find({
        selector: {
          $fts: probeTerm
        },
        limit: 1
      }).exec();
      if (readyProbe.length > 0) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  const resultViaSelector = await collections.items.find({
    selector: {
      $fts: query
    },
    limit: 10
  }).exec();

  const resultViaHelper = await collections.items.fts(query, {
    title: {
      $gt: ''
    }
  }).exec();

  printResults(query, resultViaSelector);
  console.log(`\n.fts() helper returned ${resultViaHelper.length} docs.`);

  await db.close();
}

run().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
