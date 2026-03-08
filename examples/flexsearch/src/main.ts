import {
    addRxPlugin,
    createRxDatabase
} from 'rxdb/plugins/core/index.mjs';
import { getRxStorageDexie } from 'rxdb/plugins/storage-dexie/index.mjs';
import {
    getFlexSearchState,
    RxDBFlexSearchPlugin,
    wrappedFlexSearchStorage,
    type FlexSearchMetaDocumentData
} from 'rxdb/plugins/flexsearch/index.mjs';

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

const loadButton = document.getElementById('load-button') as HTMLButtonElement;
const clearButton = document.getElementById('clear-button') as HTMLButtonElement;
const searchButton = document.getElementById('search-button') as HTMLButtonElement;
const queryInput = document.getElementById('query-input') as HTMLInputElement;
const datasetStatus = document.getElementById('dataset-status') as HTMLElement;
const snapshotStatus = document.getElementById('snapshot-status') as HTMLElement;
const resultCount = document.getElementById('result-count') as HTMLElement;
const results = document.getElementById('results') as HTMLElement;

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
    const response = await fetch('/public/items.transformed.json');
    if (!response.ok) {
        throw new Error('Dataset file missing. Run `deno task prepare-data` in examples/flexsearch first.');
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
    
    // Don't wait for initPromise here - it creates a deadlock!
    // This function is called during initial indexing, so init is already in progress.
    
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

function renderResults(query: string, docs: WikiDoc[], searchTimeMs?: number) {
    const timing = searchTimeMs !== undefined ? ` (${searchTimeMs}ms)` : '';
    resultCount.textContent = `${docs.length} matches for "${query}"${timing}`;
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
    console.log('[loadData] Starting...');
    loadButton.disabled = true;
    datasetStatus.textContent = 'Loading dataset...';

    try {
        console.log('[loadData] Getting database...');
        const db = await getDatabase();
        console.log('[loadData] Database obtained, counting docs...');
        const existing = await db.items.count().exec();
        console.log(`[loadData] Existing docs: ${existing}`);
        
        if (existing === 0) {
            console.log('[loadData] Loading dataset file...');
            const docs = await loadDatasetFile();
            console.log(`[loadData] Dataset loaded: ${docs.length} docs`);
            
            console.log('[loadData] Bulk inserting...');
            await db.items.bulkInsert(docs);
            console.log('[loadData] Bulk insert complete');
            
            const probeTerm = docs[0]?.title.split(/\s+/)[0];
            if (probeTerm) {
                console.log(`[loadData] Waiting for index: "${probeTerm}"...`);
                await waitUntilIndexed(probeTerm);
                console.log('[loadData] Index ready');
            }
        }

        console.log('[loadData] Updating status...');
        await updateStatus();
        console.log('[loadData] Status updated');
        
        // Wait a moment for persistence to trigger, then update status again
        setTimeout(async () => {
            console.log('[loadData] Updating status (delayed)...');
            await updateStatus();
            console.log('[loadData] Status updated (delayed)');
        }, 1500);
    } catch (error) {
        console.error('[loadData] Error:', error);
        datasetStatus.textContent = 'Error loading';
    } finally {
        console.log('[loadData] Re-enabling button');
        loadButton.disabled = false;
    }
}

async function runSearch() {
    const query = queryInput.value.trim();
    if (!query) {
        renderResults('', []);
        return;
    }

    const db = await getDatabase();

    // Measure search time
    const searchStart = performance.now();
    const docs = await db.items.fts(query).exec();
    const searchTime = Math.round(performance.now() - searchStart);
    
    renderResults(query, docs.slice(0, 12), searchTime);
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