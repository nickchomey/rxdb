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
    titleLength?: number;
};

type WikiCollection = {
    items: {
        bulkInsert(docs: WikiDoc[]): Promise<unknown>;
        count(): { exec(): Promise<number> };
        find(query: { selector: Record<string, unknown>; limit?: number }): { exec(): Promise<WikiDoc[]> };
        findByIds(ids: string[]): { exec(): Promise<Map<string, any>> };
        fts(queryOrOptions?: string | Record<string, unknown>, selector?: Record<string, unknown>): { exec(): Promise<WikiDoc[]> };
    };
};

type WikiDatabase = {
    name: string;
    items: WikiCollection['items'];
    addCollections(collections: Record<string, unknown>): Promise<unknown>;
    remove(): Promise<void>;
};

addRxPlugin(RxDBFlexSearchPlugin);

const loadButton = document.getElementById('load-button') as HTMLButtonElement;
const clearButton = document.getElementById('clear-button') as HTMLButtonElement;
const searchButton = document.getElementById('search-button') as HTMLButtonElement;
const queryInput = document.getElementById('query-input') as HTMLInputElement;
const datasetStatus = document.getElementById('dataset-status') as HTMLElement;
const snapshotStatus = document.getElementById('snapshot-status') as HTMLElement;
const resultsTitle = document.getElementById('results-title') as HTMLElement;
const resultCount = document.getElementById('result-count') as HTMLElement;
const results = document.getElementById('results') as HTMLElement;
const addDocButton = document.getElementById('add-doc-button') as HTMLButtonElement;
const docTitleInput = document.getElementById('doc-title') as HTMLInputElement;
const docContentInput = document.getElementById('doc-content') as HTMLTextAreaElement;
const addDocStatus = document.getElementById('add-doc-status') as HTMLElement;

let dbPromise: Promise<WikiDatabase> | undefined;

async function getDatabase(): Promise<WikiDatabase> {
    if (!dbPromise) {
        dbPromise = createRxDatabase<WikiCollection>({
            name: 'rxdb-flexsearch-example-web',
            storage: wrappedFlexSearchStorage({
                storage: getRxStorageDexie(),
                debug: true,
                persistence: {
                    minDebounce: 150,
                    maxDebounce: 1200,
                    adaptive: true
                }
            }),
            multiInstance: false,
            eventReduce: true,
            ignoreDuplicate: false
        }).then(async (db: WikiDatabase) => {
            await db.addCollections({
                items: {
                    schema: {
                        version: 0,
                                                version: 1,
                        primaryKey: 'id',
                                                version: 1,
                                                primaryKey: 'id',
                        type: 'object',
                                            schema: {
                                                version: 1,
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
                                    // Higher resolution boosts title matches above content in scoring
                                    tokenize: 'forward',
                                    resolution: 9
                                }
                                                        version: 1,
                                type: 'string',
                                fts: {
                                    tokenize: 'forward',
                                    resolution: 3
                                }
                            },
                            titleLength: {
                                type: 'number'
                            }
                        },
                        required: ['id', 'title', 'content']
                    }
                }
            });

            const state = getFlexSearchState(db.name, 'items');
            if (state?.initPromise) {
                void state.initPromise.then(() => {
                    void updateStatus();
                });
            }

            return db;
        });
    }
    return dbPromise as Promise<WikiDatabase>;
}

async function loadDatasetFile(): Promise<WikiDoc[]> {
    const response = await fetch('/public/items.transformed.json');
    if (!response.ok) {
        throw new Error('Dataset file missing. Run `deno task prepare-data` in examples/flexsearch first.');
    }
    return await response.json() as WikiDoc[];
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
        snapshotStatus.textContent = state.initPromise
            ? 'Initializing snapshot...'
            : 'No persisted snapshot yet';
        return;
    }

    snapshotStatus.textContent = `${meta.serialized.length.toLocaleString()} serialized bytes persisted`;
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
    if (searchTimeMs !== undefined) {
        resultsTitle.textContent = `Results (${searchTimeMs} ms)`;
    } else {
        resultsTitle.textContent = 'Results';
    }
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

    try {
        const db = await getDatabase();
        const existing = await db.items.count().exec();

        if (existing === 0) {
            let docs = await loadDatasetFile();
            // Add titleLength for testing where clauses with $in optimization
            docs = docs.map(doc => ({
                ...doc,
                titleLength: doc.title.length
            }));
            await db.items.bulkInsert(docs);

            const probeTerm = docs[0]?.title.split(/\s+/)[0];
            if (probeTerm) {
                await waitUntilIndexed(probeTerm);
            }
        }

        const state = getFlexSearchState(db.name, 'items');
        if (state?.writeQueue) {
            await state.writeQueue;
        }

        await updateStatus();

        // Wait a moment for persistence to trigger, then update status again
        setTimeout(async () => {
            await updateStatus();
        }, 1500);
    } catch (error) {
        console.error('[loadData] Error:', error);
        datasetStatus.textContent = 'Error loading';
    } finally {
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

    // Measure search time with detailed breakdown
    const searchStart = performance.now();
    console.time('[FlexSearch App] fts() total');
    // Search with suggest:true for fuzzy/tolerant matching, and filter by titleLength > 5.
    // A per-field descriptor array applies a 5× boost to the title field so that
    // articles whose title matches the query rank above those matching only in content.
    const docs = await db.items.fts({
        suggest: true,
        field: [
            { field: 'title', query, boost: 5 },
            { field: 'content', query }
        ]
    }, { titleLength: { $gt: 5 } }).exec();
    console.timeEnd('[FlexSearch App] fts() total');
    const searchTime = Math.round(performance.now() - searchStart);

    console.log(`[FlexSearch App] Search completed in ${searchTime}ms: ${docs.length} results from query "${query}"`);

    renderResults(query, docs, searchTime);
    await updateStatus();
}

async function clearDatabase() {
    const db = await getDatabase();
    await db.remove();
    dbPromise = undefined;
    results.innerHTML = '';
    resultsTitle.textContent = 'Results';
    resultCount.textContent = '0 matches';
    datasetStatus.textContent = 'Cleared';
    snapshotStatus.textContent = 'Cleared';
}

async function addDocument() {
    const title = docTitleInput.value.trim();
    const content = docContentInput.value.trim();

    if (!title || !content) {
        addDocStatus.textContent = 'Both title and content are required.';
        addDocStatus.className = 'add-doc-status error';
        return;
    }

    addDocButton.disabled = true;
    addDocStatus.textContent = 'Adding…';
    addDocStatus.className = 'add-doc-status';

    try {
        const db = await getDatabase();
        const id = `custom-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        await db.items.bulkInsert([{ id, title, content, titleLength: title.length }]);
    await db.items.bulkInsert([{ id, title, content }]);

        docTitleInput.value = '';
        docContentInput.value = '';
        addDocStatus.textContent = `Added "${title}" — FlexSearch will index it via the change-stream.`;
        addDocStatus.className = 'add-doc-status success';

        await updateStatus();
    } catch (error) {
        console.error('[addDocument] Error:', error);
        addDocStatus.textContent = String(error);
        addDocStatus.className = 'add-doc-status error';
    } finally {
        addDocButton.disabled = false;
    }
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

addDocButton.addEventListener('click', () => {
    void addDocument();
});

docTitleInput.addEventListener('keydown', event => {
    if (event.key === 'Enter') {
        void addDocument();
    }
});

await updateStatus();