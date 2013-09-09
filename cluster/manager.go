package cluster

import "time"

/* There should be a separate manager for each service */
type Manager interface {
	GetEra(t time.Time) Era
	GetCurrentEra() Era
}

type StaticManager struct {
	E Era
}

func (c *StaticManager) GetEra(t time.Time) Era {
	return c.E
}

func (c *StaticManager) GetCurrentEra() Era {
	return c.E
}

func NewStaticManager(e Era) *StaticManager {
	return &StaticManager{e}
}
