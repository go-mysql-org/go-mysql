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

	// ensure all relay log done
	WaitRelayLogDone(s *Server) error

	// Wait until slave s catch all data from master m
	WaitCatchMaster(s *Server, m *Server) error
}
