# PR Description: Canal table memory cache optimization (#543)

## Summary
This proposal addresses memory growth risk in canal table metadata caching by introducing a bounded caching strategy. The current cache behavior can retain table entries indefinitely in long-running or high-churn workloads, which may lead to avoidable memory waste.

Issue: #543

## Problem statement
Canal maintains table-related metadata in memory for performance, but with unbounded retention:
- instances observing many schemas/tables over time can accumulate stale entries,
- memory usage may grow beyond practical limits in dynamic environments,
- cache entries that are no longer hot continue occupying heap.

The issue request suggests evaluating `LRU` or local persistence (`localdb`) to reduce waste.

## Root cause
The table memory cache lifecycle is not constrained by eviction or compaction policy. This favors hit-rate but provides no upper bound on resident table metadata footprint.

## Goals
- Bound memory usage of table metadata cache.
- Preserve fast access for hot tables.
- Avoid behavior regression in CDC correctness.
- Keep migration/rollout low-risk.

## Proposed approach
Use a two-tier strategy:
1. **Primary:** LRU-based in-memory cache with configurable capacity.
2. **Optional extension:** local persistent backing store for cold metadata (feature-gated), used only if operationally required.

### Why LRU first
- Lowest implementation and operational complexity.
- Immediate memory bound with predictable behavior.
- Keeps hot-path latency low for frequently accessed tables.

### Why localdb optional
- Useful for extreme cardinality scenarios.
- Introduces I/O and durability considerations; therefore should be incremental and opt-in.

## Design outline
- Replace/augment current unbounded table cache map with LRU container.
- Add configuration knobs (example names):
  - `canal.table_cache.capacity` (max entries),
  - `canal.table_cache.enable_lru` (default true once stable),
  - `canal.table_cache.localdb.enabled` (default false).
- Maintain existing lookup semantics from caller perspective.
- On eviction, drop only cacheable metadata artifacts (no data-loss semantics).

## Compatibility and behavior
- Backward-compatible defaults should preserve current functionality.
- In workloads below capacity, behavior is effectively unchanged.
- Above capacity, cold entries are evicted and lazily reloaded on next access.

## Testing plan
- Unit tests:
  - cache hit/miss behavior parity,
  - deterministic LRU eviction order,
  - capacity boundary and churn scenarios.
- Integration tests:
  - canal run against rotating table sets,
  - verify no CDC event correctness regressions,
  - monitor memory plateau under sustained churn.
- Performance checks:
  - compare steady-state memory footprint,
  - ensure acceptable cache-miss penalty.

## Risks and mitigations
- **Risk:** Increased misses for rarely used tables.
  - **Mitigation:** conservative default capacity + tuning docs.
- **Risk:** Latency spikes during metadata reload.
  - **Mitigation:** benchmark and expose capacity configuration.
- **Risk (if localdb enabled):** storage I/O overhead.
  - **Mitigation:** keep localdb disabled by default; feature gate.

## Rollout plan
1. Introduce LRU implementation behind config flag.
2. Validate in representative workloads.
3. Set default to enabled after confidence.
4. Keep optional localdb path for future follow-up if needed.

## Operational notes
- Expose cache metrics (entry count, evictions, misses) to simplify tuning.
- Document recommended capacity sizing by table cardinality and memory budget.

## Issue linkage
- Closes: #543
