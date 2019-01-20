package canal

type Observer struct {
	BeforeSchemaChange   func(string, string) error
	OnSchemaChangeFailed func(string, string, error) (bool, error)
}

// Register a hook that will be called before schema change
func (c *Canal) RegisterBeforeSchemaChangeHook(fn func(string, string) error) {
	c.observer.BeforeSchemaChange = fn
}

// Register a hook that will be called on DDL failed
func (c *Canal) RegisterOnSchemaChangeFailedHook(fn func(string, string, error) (bool, error)) {
	c.observer.OnSchemaChangeFailed = fn
}

func (c *Canal) runBeforeSchemaChangeHook(db string, statement string) error {
	if c.observer.BeforeSchemaChange == nil {
		return nil
	}
	return c.observer.BeforeSchemaChange(db, statement)
}

func (c *Canal) runOnSchemaChangeFailedHook(db string, statement string, err error) (bool, error) {
	if c.observer.OnSchemaChangeFailed == nil {
		return false, err
	}
	return c.observer.OnSchemaChangeFailed(db, statement, err)
}
