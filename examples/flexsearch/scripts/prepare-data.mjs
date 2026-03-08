import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const sourcePath = path.resolve(__dirname, '../../../../javascript-vector-database/files/items.json');
const outputDir = path.resolve(__dirname, '../public');
const outputPath = path.resolve(outputDir, 'items.transformed.json');

function splitBody(body) {
    const titleMarker = 'Title:';
    const contentMarker = 'Content:';

    if (typeof body !== 'string') {
        return {
            title: '',
            content: ''
        };
    }

    const titleStart = body.indexOf(titleMarker);
    const contentStart = body.indexOf(contentMarker);

    if (titleStart >= 0 && contentStart > titleStart) {
        return {
            title: body.slice(titleStart + titleMarker.length, contentStart).trim(),
            content: body.slice(contentStart + contentMarker.length).trim()
        };
    }

    return {
        title: body.slice(0, 120).trim(),
        content: body.trim()
    };
}

async function run() {
    const raw = await fs.readFile(sourcePath, 'utf8');
    const items = JSON.parse(raw);

    const transformed = items.map(item => {
        const parsed = splitBody(item.body);
        return {
            id: String(item.id),
            title: parsed.title,
            content: parsed.content
        };
    });

    await fs.mkdir(outputDir, { recursive: true });
    await fs.writeFile(outputPath, JSON.stringify(transformed), 'utf8');
    console.log(`Wrote ${transformed.length} docs to ${outputPath}`);
}

run().catch(error => {
    console.error('Failed to transform dataset:', error);
    process.exitCode = 1;
});