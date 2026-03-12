#!/usr/bin/env node
/**
 * Pre-bundle rxdb + transitive deps for Deno using esbuild.
 * Solves:
 *  - mingo sideEffects tree-shaking (--ignore-annotations bypasses it)
 *  - Transitive dep resolution in deno bundle
 * Output: dist/esm-bundled/ (only rxjs and flexsearch remain as bare imports)
 */

import { execSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.join(__dirname, '..');

const ESBUILD = path.join(projectRoot, 'node_modules', '.bin', 'esbuild');
const RXDB_ESM = path.join(projectRoot, 'dist', 'esm');
const OUT = path.join(projectRoot, 'dist', 'esm-bundled');

const entryPoints = [
    path.join(RXDB_ESM, 'index.js'),
    path.join(RXDB_ESM, 'plugins', 'flexsearch', 'index.js'),
    path.join(RXDB_ESM, 'plugins', 'replication', 'index.js'),
    path.join(RXDB_ESM, 'plugins', 'key-compression', 'index.js'),
    path.join(RXDB_ESM, 'plugins', 'dev-mode', 'index.js'),
    path.join(RXDB_ESM, 'plugins', 'validate-ajv', 'index.js'),
];

console.log('[esbuild-bundle] Bundling rxdb for Deno...');
console.log(`[esbuild-bundle] Input: ${RXDB_ESM}`);
console.log(`[esbuild-bundle] Output: ${OUT}`);

const cmd = [
    `"${ESBUILD}"`,
    ...entryPoints.map(ep => `"${ep}"`),
    '--bundle',
    '--format=esm',
    '--platform=browser',
    '--external:rxjs',
    '--external:flexsearch',
    '--splitting',
    '--ignore-annotations',
    `--outdir="${OUT}"`,
].join(' ');

try {
    execSync(cmd, { stdio: 'inherit', shell: true });
    console.log('[esbuild-bundle] Done');
    process.exit(0);
} catch (err) {
    console.error('[esbuild-bundle] Failed:', err.message);
    process.exit(1);
}
