package appbase

import (
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/logging"
)

// Service base struct for typical service objects
type Service struct {
	// ID is used as [ID] prefix in log and error messages
	ID string
}

func NewServiceBase(id string) Service {
	return Service{
		ID: id,
	}
}

func (sb *Service) NewError(format string, a ...any) error {
	return fmt.Errorf("[%s] "+format, sb.args(a)...)
}

func (sb *Service) Infof(format string, a ...any) {
	logging.Infof("[%s] "+format, sb.args(a)...)
}

func (sb *Service) Errorf(format string, a ...any) {
	logging.Errorf("[%s] "+format, sb.args(a)...)
}

func (sb *Service) Warnf(format string, a ...any) {
	logging.Warnf("[%s] "+format, sb.args(a)...)
}

func (sb *Service) Debugf(format string, a ...any) {
	if logging.IsDebugEnabled() {
		logging.Debugf("[%s] "+format, sb.args(a)...)
	}
}

func (sb *Service) Fatalf(format string, a ...any) {
	logging.Fatalf("[%s] "+format, sb.args(a)...)
}

func (sb *Service) SystemErrorf(format string, a ...any) {
	logging.SystemErrorf("[%s] "+format, sb.args(a)...)
}

func (sb *Service) args(a []any) []any {
	b := make([]any, len(a)+1)
	copy(b[1:], a)
	b[0] = sb.ID
	return b
}
