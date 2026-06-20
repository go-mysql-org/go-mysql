# PR Description: Canal table-cache bounding and RunFrom startup recovery
## Summary
This branch improves Canal in two production pain points:
- bound table metadata cache growth to reduce long-running memory pressure (#543),
- recover startup when `RunFrom` points to an impossible binlog position (#642).
## Motivation
Two independent failure modes were observed in real deployments:
- Table metadata cache can grow unbounded under high schema/table churn.
- Restart from stale checkpoints can fail with:
  `Client requested master to start replication from impossible position ...`
Both cause operational instability (memory creep or startup failure) and require manual intervention.
## Changes included
### 1) Bounded table metadata cache (#543)
- Added `Config.TableCacheCapacity` (default `1024`) in `canal/config.go`.
- Added LRU bookkeeping in Canal cache management and integrated it into:
  - `GetTable`,
  - `SetTableCache`,
  - `ClearTableCache`.
- When cache size exceeds capacity, least-recently-used table metadata is evicted.
- `TableCacheCapacity <= 0` preserves unbounded behavior for backward compatibility.
- `DiscardNoMetaRowEvent` cleanup behavior remains consistent for evicted keys.
### 2) `RunFrom` impossible-position recovery (#642)
- Added impossible-position detection helper (`isImpossibleBinlogPositionError`).
- In startup path (`startSyncer`), when `StartSync(pos)` fails with impossible-position error:
  - fetch current valid master position (`GetMasterPos()`),
  - retry sync from recovered position,
  - update in-memory master position and emit recovery logs.
- Added startup-recovery helpers for error parsing/retry:
  - `fallbackStartPosFromImpossiblePositionError`,
  - `retrySyncFromImpossiblePosition`,
  to cover cases where startup errors surface during the initial stream read path.
## Compatibility and behavior
- Valid `RunFrom` positions keep existing behavior.
- Non-impossible-position errors are still returned (no broad suppression).
- Cache remains configurable; unbounded mode is retained via capacity <= 0.
## Tests and validation
- Added `canal/table_cache_test.go`:
  - LRU eviction order,
  - cache-clear behavior with LRU tracking.
- Added `canal/sync_test.go` coverage for impossible-position detection and fallback parsing.
- Targeted validation run:
  - `go test ./canal -run 'TestGetShowBinaryLogQuery|TestIsImpossibleBinlogPositionError|TestFallbackStartPosFromImpossiblePositionError'` ✅
- Full repository validation run:
  - `go test ./...` ✅
- Integration tests now skip gracefully when required local dependencies are unavailable (for example local MySQL or `mysqldump`) instead of hard-failing the whole suite.
## Risk assessment
- Cache capacity set too low can increase metadata reload frequency.
- Recovery logic is intentionally constrained to explicit impossible-position signatures to avoid masking unrelated failures.
## Issue linkage
- Closes #543
- Closes #642
