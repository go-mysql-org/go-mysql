// Failover supports to promote a new master and let other slaves
// replicate from it automatically.
//
// Failover does not support monitoring whether a master is alive or not,
// and will think the master is down.
//
// Failover will support file-position-based replication and GTID replication.
// Using GTID is easy for refactoring the replication topologies when the master is down,
// but file-position-based is not.
// There are some ways to solve it, like MHA using comparing and syncing relay log at first, but
// I think using a Pseudo GTID like [orchestrator](https://github.com/outbrain/orchestrator) may be
// more easy, only with little limitation (must open log-slave-update in slave).
package failover
