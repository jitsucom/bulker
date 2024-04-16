package api_based

import (
	"context"
	"errors"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"io"
	"os"
	"time"
)

type ApiImplementation interface {
	io.Closer
	Type() string
	Upload(fileReader io.ReadSeeker) (string, error)
}

type ApiBasedStream struct {
	id             string
	options        bulker.StreamOptions
	implementation ApiImplementation

	batchFile     *os.File
	marshaller    types2.Marshaller
	eventsInBatch int

	state  bulker.State
	inited bool

	startTime time.Time
}

func NewTransactionalStream(id string, impl ApiImplementation, streamOptions ...bulker.StreamOption) (*ApiBasedStream, error) {
	ps := ApiBasedStream{id: id, implementation: impl}
	ps.options = bulker.StreamOptions{}
	for _, option := range streamOptions {
		ps.options.Add(option)
	}
	ps.state = bulker.State{Status: bulker.Active, Representation: map[string]string{
		"name": impl.Type(),
	}}
	ps.startTime = time.Now()
	return &ps, nil
}

func (ps *ApiBasedStream) init(ctx context.Context) error {
	if ps.inited {
		return nil
	}

	if ps.batchFile == nil {
		var err error
		ps.batchFile, err = os.CreateTemp("", fmt.Sprintf("bulker_%s", utils.SanitizeString(ps.id)))
		if err != nil {
			return err
		}
		ps.marshaller, _ = types2.NewMarshaller(types2.FileFormatNDJSON, types2.FileCompressionNONE)
	}
	ps.inited = true
	return nil
}

func (ps *ApiBasedStream) preprocess(object types2.Object) (types2.Object, error) {
	ps.state.ProcessedRows++
	return object, nil
}

func (ps *ApiBasedStream) postConsume(err error) error {
	if err != nil {
		ps.state.ErrorRowIndex = ps.state.ProcessedRows
		ps.state.SetError(err)
		return err
	} else {
		ps.state.SuccessfulRows++
	}
	return nil
}

func (ps *ApiBasedStream) postComplete(err error) (bulker.State, error) {
	_ = ps.batchFile.Close()
	_ = os.Remove(ps.batchFile.Name())
	if err != nil {
		ps.state.SetError(err)
		ps.state.Status = bulker.Failed
	} else {
		sec := time.Since(ps.startTime).Seconds()
		logging.Infof("[%s] Stream completed successfully in %.2f s. Avg Speed: %.2f events/sec.", ps.id, sec, float64(ps.state.SuccessfulRows)/sec)
		ps.state.Status = bulker.Completed
	}
	return ps.state, err
}

func (ps *ApiBasedStream) flushBatchFile(ctx context.Context) (err error) {
	defer func() {
		_ = ps.batchFile.Close()
		_ = os.Remove(ps.batchFile.Name())
	}()
	if ps.eventsInBatch > 0 {
		err = ps.marshaller.Flush()
		if err != nil {
			return errorj.Decorate(err, "failed to flush marshaller")
		}
		err = ps.batchFile.Sync()
		if err != nil {
			return errorj.Decorate(err, "failed to sync batch file")
		}
		stat, _ := ps.batchFile.Stat()
		var batchSizeMb float64
		if stat != nil {
			batchSizeMb = float64(stat.Size()) / 1024 / 1024
			sec := time.Since(ps.startTime).Seconds()
			logging.Infof("[%s] Flushed %d events to batch file. Size: %.2f mb in %.2f s. Speed: %.2f mb/s", ps.id, ps.eventsInBatch, batchSizeMb, sec, batchSizeMb/sec)
		}
		workingFile := ps.batchFile
		//create file reader for workingFile
		_, err = workingFile.Seek(0, 0)
		if err != nil {
			return errorj.Decorate(err, "failed to seek to beginning of tmp file")
		}

		loadTime := time.Now()
		resp, err := ps.implementation.Upload(workingFile)
		if err != nil {
			return errorj.Decorate(err, "failed to upload data to "+ps.implementation.Type())
		} else {
			ps.state.Representation = map[string]string{
				"name":     ps.implementation.Type(),
				"response": resp,
			}
			logging.Infof("[%s] Batch file loaded to %s in %.2f s. Response: %s", ps.id, ps.implementation.Type(), time.Since(loadTime).Seconds(), resp)
		}
	}
	return nil
}

func (ps *ApiBasedStream) writeToBatchFile(ctx context.Context, processedObject types2.Object) error {
	ps.marshaller.Init(ps.batchFile, nil)
	err := ps.marshaller.Marshal(processedObject)
	if err != nil {
		return errorj.Decorate(err, "failed to marshall into json file")
	}
	ps.eventsInBatch++
	return nil
}

func (ps *ApiBasedStream) Consume(ctx context.Context, object types2.Object) (state bulker.State, processedObject types2.Object, err error) {
	defer func() {
		err = ps.postConsume(err)
		state = ps.state
	}()
	if err = ps.init(ctx); err != nil {
		return
	}

	//type mapping, flattening => table schema
	processedObject, err = ps.preprocess(object)
	if err != nil {
		return
	}

	err = ps.writeToBatchFile(ctx, processedObject)

	return
}

func (ps *ApiBasedStream) Abort(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	if ps.batchFile != nil {
		_ = ps.batchFile.Close()
		_ = os.Remove(ps.batchFile.Name())
	}
	ps.state.Status = bulker.Aborted
	return ps.state, err
}

func (ps *ApiBasedStream) Complete(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	defer func() {
		state, err = ps.postComplete(err)
	}()
	if ps.state.LastError == nil {
		//if at least one object was inserted
		if ps.state.SuccessfulRows > 0 {
			if ps.batchFile != nil {
				if err = ps.flushBatchFile(ctx); err != nil {
					return ps.state, err
				}
			}
		}
		return
	} else {
		//if was any error - it will trigger transaction rollback in defer func
		err = ps.state.LastError
		return
	}
}
