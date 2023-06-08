package objects

import (
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/logging"
)

// ServiceBase base struct for typical service objects
type ServiceBase struct {
	// ID is used as [ID] prefix in log and error messages
	ID string
}

func NewServiceBase(id string) ServiceBase {
	return ServiceBase{
		ID: id,
	}
}

func (sb *ServiceBase) NewError(format string, a ...any) error {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	return fmt.Errorf("[%s] "+format, args...)
}

func (sb *ServiceBase) Infof(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.Infof("[%s] "+format, args...)
}

func (sb *ServiceBase) Errorf(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.Errorf("[%s] "+format, args...)
}

func (sb *ServiceBase) Warnf(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.Warnf("[%s] "+format, args...)
}

func (sb *ServiceBase) Debugf(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.Debugf("[%s] "+format, args...)
}

func (sb *ServiceBase) Fatalf(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.Fatalf("[%s] "+format, args...)
}

func (sb *ServiceBase) SystemErrorf(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.SystemErrorf("[%s] "+format, args...)
}
