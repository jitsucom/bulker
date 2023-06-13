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
	args := []interface{}{sb.ID}
	args = append(args, a...)
	return fmt.Errorf("[%s] "+format, args...)
}

func (sb *Service) Infof(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.Infof("[%s] "+format, args...)
}

func (sb *Service) Errorf(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.Errorf("[%s] "+format, args...)
}

func (sb *Service) Warnf(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.Warnf("[%s] "+format, args...)
}

func (sb *Service) Debugf(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.Debugf("[%s] "+format, args...)
}

func (sb *Service) Fatalf(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.Fatalf("[%s] "+format, args...)
}

func (sb *Service) SystemErrorf(format string, a ...any) {
	args := []interface{}{sb.ID}
	args = append(args, a...)
	logging.SystemErrorf("[%s] "+format, args...)
}
