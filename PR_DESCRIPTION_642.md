# PR Description: canal `RunFrom` impossible binlog position handling (#642)

## Summary
This PR addresses canal startup failures when `RunFrom` is initialized with a binlog file/offset pair that is no longer valid on the upstream server.

Typical error:

`Client requested master to start replication from impossible position; the first event 'binlog.024078' at 1247892405, the last event read from 'binlog.024078' at 4, the last byte read from 'binlog.024078' at 4.`

The goal is to make startup behavior deterministic and recoverable for this class of invalid checkpoint.

## Background / user impact
In production, teams often run multiple consumers against the same binlog stream for parallel processing.  
When one consumer restarts with an outdated or inconsistent position, canal currently fails fast at startup and cannot continue syncing.

Common triggers:
- binlog purge/rotation on upstream MySQL,
- failover/topology switch,
- stale checkpoint after downtime,
- offset drift caused by external checkpoint storage mismatch.

## Root cause
`RunFrom` assumes the provided start position is valid for the current upstream binlog state.  
When MySQL rejects the requested position as impossible, startup aborts instead of entering a controlled fallback path.

## Proposed changes
- Detect the impossible-position startup failure pattern in canal initialization.
- Introduce a bounded fallback strategy to a server-acceptable start position.
- Preserve existing behavior when the provided `RunFrom` position is valid.
- Keep non-position-related replication errors visible (no broad error suppression).

## Behavior after fix
- Valid `RunFrom` inputs: no behavior change.
- Impossible/out-of-range start position: recover and continue replication.
- Unrelated replication failures: still returned to caller.

## Compatibility
- Scope limited to canal startup path around `RunFrom`.
- No public API signature changes required.
- Backward-compatible for normal valid-position workflows.

## Validation plan
- Reproduce the original impossible-position error with controlled binlog offset.
- Confirm startup now recovers and begins syncing.
- Verify valid `RunFrom` positions still behave identically.
- Verify unrelated startup failures are not masked.

## Risk and mitigation
Risk: selecting an incorrect fallback point in edge cases.

Mitigation:
- fallback only on recognized impossible-position startup errors,
- keep fallback logic narrowly scoped to startup position handling,
- keep logging explicit for observability and rollback confidence.

## Rollout notes
- Safe as a focused bug fix.
- No schema/config migration required.
- Operators should monitor startup logs to confirm fallback activation only on invalid checkpoints.

## Issue linkage
- Closes #642
