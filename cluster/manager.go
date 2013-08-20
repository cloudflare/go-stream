package cluster

import "time"

type Manager interface {
	GetEra(t time.Time) Era
}
