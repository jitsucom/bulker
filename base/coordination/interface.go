package coordination

import (
	"github.com/jitsucom/bulker/base/locks"
	"io"
)

// Service is a coordination service which is responsible for all distributed operations like:
// - distributed locks
// - obtain cluster information
type Service interface {
	io.Closer
	//TODO: remove?
	GetJitsuInstancesInCluster() ([]string, error)
	CreateLock(name string) locks.Lock
}

type DummyCoordinationService struct {
}

func (s DummyCoordinationService) GetJitsuInstancesInCluster() ([]string, error) {
	return []string{}, nil
}

func (s DummyCoordinationService) CreateLock(name string) locks.Lock {
	return locks.DummyLock{}
}

func (s DummyCoordinationService) Close() error {
	return nil
}
