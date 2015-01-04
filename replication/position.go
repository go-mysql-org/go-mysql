package replication

// For binlog filename + position based replication
type Position struct {
	Name string
	Pos  uint32
}
