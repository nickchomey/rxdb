# FlexSearch Plugin Performance Analysis & Optimizations

## Investigation Summary

Compared performance between:
- **javascript-vector-database** app: Uses `rxdb-premium/plugins/flexsearch` (official RxDB plugin)
- **Our example**: Custom plugin with multi/single-field configuration

## Findings

### 1. Index Type & Configuration Differences

**javascript-vector-database**:
- Single-field index via `docToString: (doc) => doc.body`
- Uses premiumPlugin's implementation
- Fulltext search reported as "instantaneous"

**Our Example (before optimization)**:
- Multi-field Document with `title` + `content` (both `tokenize: 'forward'`, `resolution: 9`)
- Search taking 500+ ms

### 2. Performance Breakdown

Timing analysis with instrumentation:
```
FlexSearch search() call:         0.90ms ✓ FAST
ID extraction from results:      <1ms   ✓ FAST
Query hook rewriting:            <1ms   ✓ FAST
─────────────────────────────────────────
Database query execution:       523ms   ✗ SLOW
═════════════════════════════════════════
Total fts() time:               524ms
```

**Bottleneck**: RxDB's query execution layer, not FlexSearch itself.

### 3. Optimizations Applied

#### Optimization 1: Tokenization Change
- **Change**: `tokenize: 'forward'` → `tokenize: 'strict'`
- **Effect**: Reduced index snapshot from 29MB → 8.3MB
- **Search time improvement**: Minimal (~0% on final search)
- **Known issue**: Aggressive tokenization doesn't proportionally affect query execution time

#### Optimization 2: Index Type Change
- **Change**: Multi-field Document (title + content) → Single-field composite (fulltext)
- **Implementation**: Concatenate and lowercase `title + content` into `fulltext` field
- **Effect**: Reduced snapshot from 8.3MB → 7.8MB
- **Search time improvement**: 562ms → 524ms (~7% improvement)
- **Reason**: Single field still slower than expected; RxDB query layer is still the bottleneck

### 4. Root Cause Analysis

The 500+ ms delay is NOT caused by:
- ✗ FlexSearch index search (only 0.9ms)
- ✗ Index configuration or type
- ✗ Tokenization settings

The delay IS caused by:
- ✓ RxDB's query execution layer
  - Query hook rewriting and processing
  - Database .exec() call overhead
  - Event-reduce processing for results
  - IndexedDB fetching and document materialization

### 5. Remaining Bottleneck

When `db.items.fts(query).exec()` is called:
1. Query hook: `rewriteFtsSelector()` rewrites $fts → primaryKey $in filter (fast, <1ms)
2. Database layer: RxDB executes the rewritten query (slow, 523ms)
   - Searches by document IDs in storage
   - Materializes document objects
   - Runs event-reduce pipeline
   - Returns results to caller

## Performance Comparison

| Operation | Time |
|-----------|------|
| FlexSearch index.search("Philippe") | 0.90ms |
| db.items.fts("Philippe").exec() | 524ms |
| **RxDB query overhead factor** | **580x slower** |

## Recommendations

To achieve faster search like javascript-vector-database:

1. **Investigate premium plugin** - May have optimizations in query execution layer
2. **Bypass RxDB for FTS** - Return IDs from FlexSearch, fetch documents separately:
   ```typescript
   const state = getFlexSearchState(db.name, 'items');
   const ids = state.index.search(query);
   const docs = await db.items.bulkGetByIds(ids);
   ```
3. **Optimize RxDB query pipeline** - Look into:
   - Disabling event-reduce for FTS queries
   - Using simpler primary key queries
   - Direct IndexedDB access after ID filtering
4. **Caching** - Cache search results to avoid repeated query overhead

## Current Status

✓ Search functionality working correctly
✓ 12 results returned for "Philippe" query
✓ Index efficiently persisted (~7.8MB)
✓ Snapshot restoration working
✗ Search latency 500+ ms (vs desired instant/sub-100ms)

## Files Modified

- `src/plugins/flexsearch/query-rewrite.ts` - Added timing instrumentation
- `examples/flexsearch/src/main.ts` - Changed to single-field FTS, added timing logs

## Next Steps

1. Consider direct FlexSearch result handling to bypass RxDB query layer
2. Profile RxDB query execution to identify hot paths
3. Evaluate premium plugin performance characteristics
4. Consider query result caching strategy
