package file_storage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	implementations2 "github.com/jitsucom/bulker/bulkerlib/implementations"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/utils"
	jsoniter "github.com/json-iterator/go"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

type AbstractFileStorageStream struct {
	id           string
	mode         bulker.BulkMode
	fileAdapter  implementations2.FileAdapter
	options      bulker.StreamOptions
	filenameFunc func(ctx context.Context) string

	flatten         bool
	merge           bool
	pkColumns       []string
	timestampColumn string

	batchFile          *os.File
	marshaller         types2.Marshaller
	targetMarshaller   types2.Marshaller
	eventsInBatch      int
	batchFileLinesByPK map[string]int
	batchFileSkipLines utils.Set[int]
	csvHeader          utils.Set[string]

	firstEventTime time.Time
	lastEventTime  time.Time

	state  bulker.State
	inited bool

	startTime time.Time
}

func newAbstractFileStorageStream(id string, p implementations2.FileAdapter, filenameFunc func(ctx context.Context) string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (AbstractFileStorageStream, error) {
	ps := AbstractFileStorageStream{id: id, fileAdapter: p, filenameFunc: filenameFunc, mode: mode}
	ps.options = bulker.StreamOptions{}
	for _, option := range streamOptions {
		ps.options.Add(option)
	}
	ps.merge = bulker.DeduplicateOption.Get(&ps.options)
	pkColumns := bulker.PrimaryKeyOption.Get(&ps.options)
	if ps.merge && len(pkColumns) == 0 {
		return AbstractFileStorageStream{}, fmt.Errorf("MergeRows option requires primary key option. Please provide WithPrimaryKey option")
	}
	ps.pkColumns = pkColumns.ToSlice()
	ps.timestampColumn = bulker.TimestampOption.Get(&ps.options)
	if ps.merge {
		ps.batchFileLinesByPK = make(map[string]int)
		ps.batchFileSkipLines = utils.NewSet[int]()
	}
	ps.csvHeader = utils.NewSet[string]()
	ps.state = bulker.State{Status: bulker.Active}
	ps.startTime = time.Now()
	return ps, nil
}

func (ps *AbstractFileStorageStream) init(ctx context.Context) error {
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
		ps.targetMarshaller, err = types2.NewMarshaller(ps.fileAdapter.Format(), ps.fileAdapter.Compression())
		if err != nil {
			return err
		}
		if !ps.merge && ps.fileAdapter.Format() == types2.FileFormatNDJSON {
			//without merge we can write file with compression - no need to convert
			ps.marshaller, _ = types2.NewMarshaller(ps.fileAdapter.Format(), ps.fileAdapter.Compression())
		}
		if ps.fileAdapter.Format() == types2.FileFormatCSV || ps.fileAdapter.Format() == types2.FileFormatNDJSONFLAT {
			ps.flatten = true
		}
	}
	ps.inited = true
	return nil
}

func (ps *AbstractFileStorageStream) preprocess(object types2.Object) (types2.Object, error) {
	if ps.flatten {
		flatObject, err := implementations2.NewFlattener(false, false).FlattenObject(object, nil)
		if err != nil {
			return nil, err
		} else {
			ps.state.ProcessedRows++
			return flatObject, nil
		}
	} else {
		ps.state.ProcessedRows++
		return object, nil
	}
}

func (ps *AbstractFileStorageStream) postConsume(err error) error {
	if err != nil {
		ps.state.ErrorRowIndex = ps.state.ProcessedRows
		ps.state.SetError(err)
		return err
	} else {
		ps.state.SuccessfulRows++
	}
	return nil
}

func (ps *AbstractFileStorageStream) postComplete(err error) (bulker.State, error) {
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

func (ps *AbstractFileStorageStream) flushBatchFile(ctx context.Context) (err error) {
	defer func() {
		if ps.merge {
			ps.batchFileLinesByPK = make(map[string]int)
			ps.batchFileSkipLines = utils.NewSet[int]()
		}
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
		needToConvert := false
		convertStart := time.Now()
		if !ps.targetMarshaller.Equal(ps.marshaller) {
			needToConvert = true
		}
		if len(ps.batchFileSkipLines) > 0 || needToConvert {
			workingFile, err = os.CreateTemp("", path.Base(ps.batchFile.Name())+"_2")
			if err != nil {
				return errorj.Decorate(err, "failed to create tmp file for deduplication")
			}
			defer func() {
				_ = workingFile.Close()
				_ = os.Remove(workingFile.Name())
			}()
			if needToConvert {
				header := ps.csvHeader.ToSlice()
				sort.Strings(header)
				err = ps.targetMarshaller.Init(workingFile, header)
				if err != nil {
					return errorj.Decorate(err, "failed to write header for converted batch file")
				}
			}
			file, err := os.Open(ps.batchFile.Name())
			if err != nil {
				return errorj.Decorate(err, "failed to open tmp file")
			}
			defer func() {
				_ = file.Close()
			}()
			scanner := bufio.NewScanner(file)
			scanner.Buffer(make([]byte, 1024*100), 1024*1024*10)
			i := 0
			for scanner.Scan() {
				if !ps.batchFileSkipLines.Contains(i) {
					if needToConvert {
						dec := jsoniter.NewDecoder(bytes.NewReader(scanner.Bytes()))
						dec.UseNumber()
						obj := make(map[string]any)
						err = dec.Decode(&obj)
						if err != nil {
							return errorj.Decorate(err, "failed to decode json object from batch filer")
						}
						err = ps.targetMarshaller.Marshal(obj)
						if err != nil {
							return errorj.Decorate(err, "failed to marshall object to target format")
						}
					} else {
						_, err = workingFile.Write(scanner.Bytes())
						if err != nil {
							return errorj.Decorate(err, "failed write to deduplication file")
						}
						_, _ = workingFile.Write([]byte("\n"))
					}
				}
				i++
			}
			if err = scanner.Err(); err != nil {
				return errorj.Decorate(err, "failed to read batch file")
			}
			ps.targetMarshaller.Flush()
			workingFile.Sync()
		}
		if needToConvert {
			stat, _ = workingFile.Stat()
			var convertedSizeMb float64
			if stat != nil {
				convertedSizeMb = float64(stat.Size()) / 1024 / 1024
			}
			logging.Infof("[%s] Converted batch file from %s (%.2f mb) to %s (%.2f mb) in %.2f s.", ps.id, ps.marshaller.FileExtension(), batchSizeMb, ps.targetMarshaller.FileExtension(), convertedSizeMb, time.Since(convertStart).Seconds())
		}
		//create file reader for workingFile
		_, err = workingFile.Seek(0, 0)
		if err != nil {
			return errorj.Decorate(err, "failed to seek to beginning of tmp file")
		}
		fileName := ps.filenameFunc(ctx)
		fileName = ps.fileAdapter.AddFileExtension(fileName)
		ps.state.Representation = map[string]string{
			"name": ps.fileAdapter.Path(fileName),
		}
		loadTime := time.Now()
		err = ps.fileAdapter.Upload(fileName, workingFile)
		if err != nil {
			return errorj.Decorate(err, "failed to flush tmp file to the warehouse")
		} else {
			logging.Infof("[%s] Batch file loaded to %s in %.2f s.", ps.id, ps.fileAdapter.Type(), time.Since(loadTime).Seconds())
		}
	}
	return nil
}

func (ps *AbstractFileStorageStream) getPKValue(object types2.Object) (string, error) {
	pkColumns := ps.pkColumns
	l := len(pkColumns)
	if l == 0 {
		return "", fmt.Errorf("primary key is not set")
	}
	if l == 1 {
		pkValue, _ := object[pkColumns[0]]
		return fmt.Sprint(pkValue), nil
	}
	pkArr := make([]string, 0, l)
	for _, col := range pkColumns {
		pkValue, _ := object[col]
		pkArr = append(pkArr, fmt.Sprint(pkValue))
	}
	return strings.Join(pkArr, "_###_"), nil
}

func (ps *AbstractFileStorageStream) writeToBatchFile(ctx context.Context, processedObject types2.Object) error {
	header := ps.csvHeader.ToSlice()
	sort.Strings(header)
	ps.marshaller.Init(ps.batchFile, header)
	if ps.merge {
		pk, err := ps.getPKValue(processedObject)
		if err != nil {
			return err
		}
		line, ok := ps.batchFileLinesByPK[pk]
		if ok {
			ps.batchFileSkipLines.Put(line)
		}
		lineNumber := ps.eventsInBatch
		if ps.marshaller.NeedHeader() {
			lineNumber++
		}
		ps.batchFileLinesByPK[pk] = lineNumber
	}
	err := ps.marshaller.Marshal(processedObject)
	if err != nil {
		return errorj.Decorate(err, "failed to marshall into csv file")
	}
	ps.eventsInBatch++
	return nil
}

func (ps *AbstractFileStorageStream) Consume(ctx context.Context, object types2.Object) (state bulker.State, processedObject types2.Object, err error) {
	defer func() {
		err = ps.postConsume(err)
		state = ps.state
	}()
	if err = ps.init(ctx); err != nil {
		return
	}
	eventTime := ps.getEventTime(object)
	if ps.lastEventTime.IsZero() || eventTime.After(ps.lastEventTime) {
		ps.lastEventTime = eventTime
	}
	if ps.firstEventTime.IsZero() || eventTime.Before(ps.firstEventTime) {
		ps.firstEventTime = eventTime
	}

	//type mapping, flattening => table schema
	processedObject, err = ps.preprocess(object)
	if err != nil {
		return
	}

	if ps.targetMarshaller.Format() == "csv" {
		ps.csvHeader.PutAllKeys(processedObject)
	}

	err = ps.writeToBatchFile(ctx, processedObject)

	return
}

func (ps *AbstractFileStorageStream) Abort(ctx context.Context) (state bulker.State, err error) {
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

func (ps *AbstractFileStorageStream) getEventTime(object types2.Object) time.Time {
	if ps.timestampColumn != "" {
		tm, ok := types2.ReformatTimeValue(object[ps.timestampColumn], false)
		if ok {
			return tm
		}
	}
	return time.Now()
}

func (ps *AbstractFileStorageStream) Complete(ctx context.Context) (state bulker.State, err error) {
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
