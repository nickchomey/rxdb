import './style.css';
import {
    addRxPlugin,
    createRxDatabase
} from '../../../plugins/core/index.mjs';
import { getRxStorageDexie } from '../../../plugins/storage-dexie/index.mjs';
import {
    getFlexSearchState,
    RxDBFlexSearchPlugin,
    wrappedFlexSearchStorage,
    type FlexSearchMetaDocumentData
} from '../../../plugins/flexsearch/index.mjs';

type WikiDoc = {
    id: string;
    title: string;
    content: string;
};

type WikiCollection = {
    items: {
        bulkInsert(docs: WikiDoc[]): Promise<unknown>;
        count(): { exec(): Promise<number> };
        find(query: { selector: Record<string, unknown>; limit?: number }): { exec(): Promise<WikiDoc[]> };
        fts(searchTerm: string, selector?: Record<string, unknown>): { exec(): Promise<WikiDoc[]> };
    };
};

addRxPlugin(RxDBFlexSearchPlugin);

document.querySelector<HTMLDivElement>('#app')!.innerHTML = `
<main class="page">
  <section class="hero">
    <p class="eyebrow">RxDB Example</p>
    <h1>FlexSearch With Persistent Snapshot Restore</h1>
    <p class="lede">
      Load the Wikipedia-derived dataset into IndexedDB via Dexie, search it with the new FlexSearch storage wrapper,
      and reload the page to verify that the index comes back from the persisted serialized snapshot instead of a full rebuild.
    </p>
  </section>

  <section class="controls card">
    <div class="button-row">
      <button id="load-button">Load Dataset</button>
      <button id="clear-button" class="ghost">Clear Database</button>
    </div>
    <label class="search-label" for="query-input">Search</label>
    <div class="search-row">
      <input id="query-input" value="Philippe" placeholder="Search title or content" />
      <button id="search-button">Search</button>
    </div>
    <div class="status-grid">
      <div>
        <span class="status-label">Dataset</span>
        <span id="dataset-status">Not loaded</span>
      </div>
      <div>
        <span class="status-label">Snapshot</span>
        <span id="snapshot-status">Unknown</span>
      </div>
      <div>
        <span class="status-label">Storage</span>
        <span>Dexie / IndexedDB</span>
      </div>
    </div>
  </section>

  <section class="results card">
    <div class="results-head">
      <h2>Results</h2>
      <span id="result-count">0 matches</span>
    </div>
    <div id="results"></div>
  </section>
</main>`;

const loadButton = document.querySelector<HTMLButtonElement>('#load-button')!;
const clearButton = document.querySelector<HTMLButtonElement>('#clear-button')!;
const searchButton = document.querySelector<HTMLButtonElement>('#search-button')!;
const queryInput = document.querySelector<HTMLInputElement>('#query-input')!;
const datasetStatus = document.querySelector<HTMLElement>('#dataset-status')!;
const snapshotStatus = document.querySelector<HTMLElement>('#snapshot-status')!;
const resultCount = document.querySelector<HTMLElement>('#result-count')!;
const results = document.querySelector<HTMLElement>('#results')!;

let dbPromise: Promise<any> | undefined;

async function getDatabase() {
    if (!dbPromise) {
        dbPromise = createRxDatabase<WikiCollection>({
            name: 'rxdb-flexsearch-example-web',
            storage: wrappedFlexSearchStorage({
                storage: getRxStorageDexie(),
                persistence: {
                    minDebounce: 150,
                    maxDebounce: 1200,
                    adaptive: true
                }
            }),
            multiInstance: false,
            eventReduce: true,
            ignoreDuplicate: false
        }).then(async db => {
            await db.addCollections({
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
            return db;
        });
    }
    return dbPromise;
}

async function loadDatasetFile(): Promise<WikiDoc[]> {
    const response = await fetch('/items.transformed.json');
    if (!response.ok) {
        throw new Error('Dataset file missing. Run `npm run prepare-data` in examples/flexsearch first.');
    }
    return await response.json();
}

async function updateStatus() {
    const db = await getDatabase();
    const count = await db.items.count().exec();
    datasetStatus.textContent = count > 0 ? `${count.toLocaleString()} docs loaded` : 'Not loaded';

    const state = getFlexSearchState(db.name, 'items');
    if (!state?.metaStorage) {
        snapshotStatus.textContent = 'No meta storage';
        return;
    }

    const docs = await state.metaStorage.findDocumentsById(['index-state'], false);
    const meta = docs[0] as FlexSearchMetaDocumentData | undefined;
    if (!meta?.serialized || meta.serialized.length === 0) {
        snapshotStatus.textContent = 'No persisted snapshot yet';
        return;
    }

    snapshotStatus.textContent = `${meta.serialized.length.toLocaleString()} compressed bytes persisted`;
}

async function waitUntilIndexed(probeTerm: string) {
    const db = await getDatabase();
    for (let i = 0; i < 80; i++) {
        const docs = await db.items.find({
            selector: {
                $fts: probeTerm
            },
            limit: 1
        }).exec();
        if (docs.length > 0) {
            return;
        }
        await new Promise(resolve => setTimeout(resolve, 100));
    }
}

function renderResults(query: string, docs: WikiDoc[]) {
    resultCount.textContent = `${docs.length} matches for "${query}"`;
    results.innerHTML = docs.map((doc, index) => {
        const preview = doc.content.replace(/\s+/g, ' ').slice(0, 260);
        return `
        <article class="result-card">
          <p class="result-index">${index + 1}</p>
          <div>
            <h3>${doc.title}</h3>
            <p>${preview}${doc.content.length > 260 ? '...' : ''}</p>
          </div>
        </article>`;
    }).join('');
}

async function loadData() {
    loadButton.disabled = true;
    datasetStatus.textContent = 'Loading dataset...';

    const db = await getDatabase();
    const existing = await db.items.count().exec();
    if (existing === 0) {
        const docs = await loadDatasetFile();
        await db.items.bulkInsert(docs);
        const probeTerm = docs[0]?.title.split(/\s+/)[0];
        if (probeTerm) {
            await waitUntilIndexed(probeTerm);
        }
    }

    await updateStatus();
    loadButton.disabled = false;
}

async function runSearch() {
    const query = queryInput.value.trim();
    if (!query) {
        renderResults('', []);
        return;
    }

    const db = await getDatabase();
    const docs = await db.items.fts(query).exec();
    renderResults(query, docs.slice(0, 12));
    await updateStatus();
}

async function clearDatabase() {
    const db = await getDatabase();
    await db.remove();
    dbPromise = undefined;
    results.innerHTML = '';
    resultCount.textContent = '0 matches';
    datasetStatus.textContent = 'Cleared';
    snapshotStatus.textContent = 'Cleared';
}

loadButton.addEventListener('click', () => {
    void loadData();
});

searchButton.addEventListener('click', () => {
    void runSearch();
});

queryInput.addEventListener('keydown', event => {
    if (event.key === 'Enter') {
        void runSearch();
    }
});

clearButton.addEventListener('click', () => {
    void clearDatabase();
});

await updateStatus();