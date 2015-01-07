package failover

type Handler interface {
	// Promote slave s to master
	Promote(s *Server) error

	// Change slave s to master m and replicate from it
	ChangeMasterTo(s *Server, m *Server) error

	// Compare slave s1, s2 and decide which one has more replicated data from master
	// 1, s1 has more
	// 0, equal
	// -1, s2 has more
	// s1 and s2 must have same master
	Compare(s1 *Server, s2 *Server) (int, error)

	// Ensure all relay log done, it will stop slave IO_THREAD
	// You must start slave again if you want to do replication continuatively
	WaitRelayLogDone(s *Server) error

	// Wait until slave s catch all data from master m at current time
	WaitCatchMaster(s *Server, m *Server) error

	// Find best slave which has the most up-to-date data from master
	// If two or more slave have the same, find the higher weight
	FindBestSlaves(slaves []*Server) ([]*Server, error)
}
