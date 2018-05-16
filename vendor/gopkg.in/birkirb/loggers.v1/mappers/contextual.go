package mappers

import "gopkg.in/birkirb/loggers.v1"

// ContextualMap maps a logger to a contextual logger interface.
type ContextualMap struct {
	AdvancedMap
	ContextualMapper
}

// NewContextualMap returns an contextual logger that is mapped via mapper.
func NewContextualMap(m ContextualMapper) *ContextualMap {
	var a ContextualMap

	if m != nil {
		if am := NewAdvancedMap(m); am != nil {
			a.AdvancedMap = *am
		}
		a.ContextualMapper = m
	}

	return &a
}

// WithField directly maps the loggers method.
func (c *ContextualMap) WithField(key string, value interface{}) loggers.Advanced {
	return c.ContextualMapper.WithField(key, value)
}

// WithFields directly maps the loggers method.
func (c *ContextualMap) WithFields(fields ...interface{}) loggers.Advanced {
	return c.ContextualMapper.WithFields(fields...)
}
