package sql

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/implementations"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/logging"
	types2 "github.com/jitsucom/bulker/jitsubase/types"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

type AbstractTransactionalSQLStream struct {
	*AbstractSQLStream
	tx            *TxSQLAdapter
	tmpTable      *Table
	existingTable *Table
	//function that generate tmp table schema based on target table schema
	tmpTableFunc       func(ctx context.Context, tableForObject *Table, object types.Object) (table *Table)
	dstTable           *Table
	batchFile          *os.File
	marshaller         types.Marshaller
	targetMarshaller   types.Marshaller
	eventsInBatch      int
	s3                 *implementations.S3
	batchFileLinesByPK map[string]int
	batchFileSkipLines types2.Set[int]
}

func newAbstractTransactionalStream(id string, p SQLAdapter, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (*AbstractTransactionalSQLStream, error) {
	abs, err := newAbstractStream(id, p, tableName, mode, streamOptions...)
	if err != nil {
		return nil, err
	}
	ps := AbstractTransactionalSQLStream{}
	ps.existingTable = &Table{}
	ps.AbstractSQLStream = abs
	if ps.merge {
		ps.batchFileLinesByPK = make(map[string]int)
		ps.batchFileSkipLines = types2.NewSet[int]()
	}
	return &ps, nil
}

func (ps *AbstractTransactionalSQLStream) init(ctx context.Context) (err error) {
	if ps.inited {
		return nil
	}
	s3 := s3BatchFileOption.Get(&ps.options)
	if s3 != nil {
		s3Config := implementations.S3Config{AccessKey: s3.AccessKeyID, SecretKey: s3.SecretKey, Bucket: s3.Bucket, Region: s3.Region, FileConfig: implementations.FileConfig{Format: ps.sqlAdapter.GetBatchFileFormat(), Compression: ps.sqlAdapter.GetBatchFileCompression()}}
		ps.s3, err = implementations.NewS3(&s3Config)
		if err != nil {
			return fmt.Errorf("failed to setup s3 client: %v", err)
		}
	}
	localBatchFile := localBatchFileOption.Get(&ps.options)
	if localBatchFile != "" && ps.batchFile == nil {
		ps.marshaller, _ = types.NewMarshaller(types.FileFormatNDJSON, types.FileCompressionNONE)
		ps.targetMarshaller, err = types.NewMarshaller(ps.sqlAdapter.GetBatchFileFormat(), ps.sqlAdapter.GetBatchFileCompression())
		if err != nil {
			return err
		}
		if !ps.merge && ps.sqlAdapter.GetBatchFileFormat() == types.FileFormatNDJSON {
			//without merge we can write file with compression - no need to convert
			ps.marshaller, _ = types.NewMarshaller(ps.sqlAdapter.GetBatchFileFormat(), ps.sqlAdapter.GetBatchFileCompression())
		}
		ps.batchFile, err = os.CreateTemp("", localBatchFile+"_*"+ps.marshaller.FileExtension())
		if err != nil {
			return err
		}
	}
	err = ps.AbstractSQLStream.init(ctx)
	if err != nil {
		return err
	}
	if ps.tx == nil {
		ps.tx, err = ps.sqlAdapter.OpenTx(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ps *AbstractTransactionalSQLStream) postComplete(ctx context.Context, err error) (bulker.State, error) {
	if ps.batchFile != nil {
		_ = ps.batchFile.Close()
		_ = os.Remove(ps.batchFile.Name())
	}
	if err != nil {
		ps.state.SuccessfulRows = 0
		if ps.tx != nil {
			if ps.tmpTable != nil {
				_ = ps.tx.Drop(ctx, ps.tmpTable, true)
			}
			_ = ps.tx.Rollback()
		}
	} else {
		sec := time.Since(ps.startTime).Seconds()
		logging.Infof("[%s] Stream completed successfully in %.2f s. Avg Speed: %.2f events/sec.", ps.id, sec, float64(ps.state.SuccessfulRows)/sec)
		if ps.tx != nil {
			if ps.tmpTable != nil {
				err = ps.tx.Drop(ctx, ps.tmpTable, true)
				if err != nil {
					logging.Errorf("[%s] Failed to drop tmp table: %v", ps.id, err)
				}
			}
			err = ps.tx.Commit()
		}
	}

	return ps.AbstractSQLStream.postComplete(err)
}

func (ps *AbstractTransactionalSQLStream) flushBatchFile(ctx context.Context) (state *bulker.WarehouseState, err error) {
	table := ps.tmpTable
	err = ps.tx.CreateTable(ctx, table)
	if err != nil {
		return nil, errorj.Decorate(err, "failed to create table")
	}
	defer func() {
		if ps.merge {
			ps.batchFileLinesByPK = make(map[string]int)
			ps.batchFileSkipLines = types2.NewSet[int]()
		}
		_ = ps.batchFile.Close()
		_ = os.Remove(ps.batchFile.Name())
	}()
	if ps.eventsInBatch > 0 {
		err = ps.marshaller.Flush()
		if err != nil {
			return nil, errorj.Decorate(err, "failed to flush marshaller")
		}
		err = ps.batchFile.Sync()
		if err != nil {
			return nil, errorj.Decorate(err, "failed to sync batch file")
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
		if ps.targetMarshaller.Format() != ps.marshaller.Format() {
			needToConvert = true
		}
		if len(ps.batchFileSkipLines) > 0 || needToConvert {
			var writer io.WriteCloser
			var gzipWriter io.WriteCloser
			workingFile, err = os.CreateTemp("", path.Base(ps.batchFile.Name())+"_*"+ps.targetMarshaller.FileExtension())
			if err != nil {
				return nil, errorj.Decorate(err, "failed to create tmp file for deduplication")
			}
			defer func() {
				_ = workingFile.Close()
				_ = os.Remove(workingFile.Name())
			}()
			writer = workingFile
			if needToConvert {
				err = ps.targetMarshaller.InitSchema(workingFile, table.ColumnNames(), ps.sqlAdapter.GetAvroSchema(table))
				if err != nil {
					return nil, errorj.Decorate(err, "failed to write header for converted batch file")
				}
			} else {
				if ps.targetMarshaller.Compression() == types.FileCompressionGZIP {
					gzipWriter = gzip.NewWriter(writer)
					writer = gzipWriter
					defer func() { _ = gzipWriter.Close() }()
				}
			}
			file, err := os.Open(ps.batchFile.Name())
			if err != nil {
				return nil, errorj.Decorate(err, "failed to open tmp file")
			}
			defer func() {
				_ = file.Close()
			}()
			scanner := bufio.NewScanner(file)
			scanner.Buffer(make([]byte, 1024*10), 1024*1024)
			i := 0
			for scanner.Scan() {
				if !ps.batchFileSkipLines.Contains(i) {
					if needToConvert {
						cfg := jsonorder.ConfigNoNumbers
						if ps.targetMarshaller.Format() != types.FileFormatAVRO {
							cfg = jsonorder.ConfigDefault
						}
						var obj types.Object
						err = cfg.Unmarshal(scanner.Bytes(), &obj)
						if err != nil {
							return nil, errorj.Decorate(err, "failed to decode json object from batch filer")
						}
						err = ps.targetMarshaller.Marshal(obj)
						if err != nil {
							return nil, errorj.Decorate(err, "failed to marshal object to converted batch file")
						}
					} else {
						_, err = writer.Write(scanner.Bytes())
						if err != nil {
							return nil, errorj.Decorate(err, "failed write to deduplication file")
						}
						_, _ = writer.Write([]byte("\n"))
					}
				}
				i++
			}
			if err = scanner.Err(); err != nil {
				return nil, errorj.Decorate(err, "failed to read batch file")
			}
			_ = ps.targetMarshaller.Flush()
			if gzipWriter != nil {
				_ = gzipWriter.Close()
			}
			_ = workingFile.Sync()
		}
		if needToConvert {
			stat, _ = workingFile.Stat()
			var convertedSizeMb float64
			if stat != nil {
				convertedSizeMb = float64(stat.Size()) / 1024 / 1024
			}
			logging.Infof("[%s] Converted batch file from %s (%.2f mb) to %s (%.2f mb) in %.2f s.", ps.id, ps.marshaller.FileExtension(), batchSizeMb, ps.targetMarshaller.FileExtension(), convertedSizeMb, time.Since(convertStart).Seconds())
		}
		loadTime := time.Now()
		if ps.s3 != nil {
			s3Config := s3BatchFileOption.Get(&ps.options)
			rFile, err := os.Open(workingFile.Name())
			if err != nil {
				return nil, errorj.Decorate(err, "failed to open tmp file")
			}
			defer func() {
				_ = rFile.Close()
			}()
			s3FileName := path.Base(workingFile.Name())
			if s3Config.Folder != "" {
				s3FileName = s3Config.Folder + "/" + s3FileName
			}
			err = ps.s3.Upload(s3FileName, rFile)
			if err != nil {
				return nil, errorj.Decorate(err, "failed to upload file to s3")
			}
			defer ps.s3.DeleteObject(s3FileName)
			logging.Infof("[%s] Batch file uploaded to s3 in %.2f s.", ps.id, time.Since(loadTime).Seconds())
			loadTime = time.Now()
			state, err = ps.tx.LoadTable(ctx, table, &LoadSource{Type: AmazonS3, Path: s3FileName, Format: ps.sqlAdapter.GetBatchFileFormat(), S3Config: s3Config})
			if err != nil {
				return state, errorj.Decorate(err, "failed to flush tmp file to the warehouse")
			} else {
				logging.Infof("[%s] Batch file loaded to %s in %.2f s.", ps.id, ps.sqlAdapter.Type(), time.Since(loadTime).Seconds())
			}
		} else {
			state, err = ps.tx.LoadTable(ctx, table, &LoadSource{Type: LocalFile, Path: workingFile.Name(), Format: ps.sqlAdapter.GetBatchFileFormat()})
			if err != nil {
				return state, errorj.Decorate(err, "failed to flush tmp file to the warehouse")
			} else {
				logging.Infof("[%s] Batch file loaded to %s in %.2f s.", ps.id, ps.sqlAdapter.Type(), time.Since(loadTime).Seconds())
			}
		}
	}
	return
}

//func (ps *AbstractTransactionalSQLStream) ensureSchema(ctx context.Context, targetTable **Table, tableForObject *Table, initTable func(ctx context.Context) (*Table, error)) (err error) {
//	needRenewTmpTable := false
//	//first object
//	if *targetTable == nil {
//		*targetTable, err = initTable(ctx)
//		if err != nil {
//			return err
//		}
//		needRenewTmpTable = true
//	} else {
//		if !tableForObject.FitsToTable(*targetTable) {
//			needRenewTmpTable = true
//			if ps.batchFile != nil {
//				logging.Infof("[%s] Table schema changed during transaction. New columns: %v", ps.id, tableForObject.Diff(*targetTable).Columns)
//				if err = ps.flushBatchFile(ctx, *targetTable, false); err != nil {
//					return err
//				}
//			}
//			(*targetTable).Columns = utils.MapPutAll(tableForObject.Columns, (*targetTable).Columns)
//		}
//	}
//	if needRenewTmpTable {
//		//adapt tmp table for new object columns if any
//		*targetTable, err = ps.tableHelper.EnsureTableWithCaching(ctx, ps.id, *targetTable)
//		if err != nil {
//			return errorj.Decorate(err, "failed to ensure temporary table")
//		}
//		if ps.batchFile != nil {
//			err = ps.marshaller.WriteHeader((*targetTable).ColumnNames(), ps.batchFile)
//			if err != nil {
//				return errorj.Decorate(err, "failed write csv header")
//			}
//		}
//	}
//	return nil
//}

func (ps *AbstractTransactionalSQLStream) writeToBatchFile(ctx context.Context, targetTable *Table, processedObject types.Object) error {
	ps.adjustTables(ctx, targetTable, processedObject)
	ps.updateRepresentationTable(ps.tmpTable)
	err := ps.marshaller.InitSchema(ps.batchFile, nil, nil)
	if err != nil {
		return err
	}
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
	err = ps.marshaller.Marshal(processedObject)
	if err != nil {
		return errorj.Decorate(err, "failed to marshall into json file")
	}
	ps.eventsInBatch++
	return nil
}

func (ps *AbstractTransactionalSQLStream) insert(ctx context.Context, targetTable *Table, processedObject types.Object) (err error) {
	ps.adjustTables(ctx, targetTable, processedObject)
	ps.updateRepresentationTable(ps.tmpTable)
	ps.tmpTable, err = ps.sqlAdapter.TableHelper().EnsureTableWithoutCaching(ctx, ps.tx, ps.id, ps.tmpTable)
	if err != nil {
		return errorj.Decorate(err, "failed to ensure table")
	}
	return ps.tx.Insert(ctx, ps.tmpTable, ps.merge, processedObject)
}

func (ps *AbstractTransactionalSQLStream) adjustTables(ctx context.Context, targetTable *Table, processedObject types.Object) {
	if ps.tmpTable == nil {
		//targetTable contains desired name and primary key setup
		ps.dstTable = targetTable
		ps.tmpTable = ps.tmpTableFunc(ctx, targetTable, processedObject)
	} else {
		ps.adjustTableColumnTypes(ps.tmpTable, ps.existingTable, targetTable, processedObject)
	}
	ps.dstTable.Columns = ps.tmpTable.Columns
}

func (ps *AbstractTransactionalSQLStream) ConsumeJSON(ctx context.Context, json []byte) (state bulker.State, processedObject types.Object, err error) {
	var obj types.Object
	err = jsonorder.Unmarshal(json, &obj)
	if err != nil {
		return ps.state, nil, fmt.Errorf("Error parsing JSON: %v", err)
	}
	return ps.Consume(ctx, obj)
}

func (ps *AbstractTransactionalSQLStream) ConsumeMap(ctx context.Context, mp map[string]any) (state bulker.State, processedObject types.Object, err error) {
	return ps.Consume(ctx, types.ObjectFromMap(mp))
}

func (ps *AbstractTransactionalSQLStream) Consume(ctx context.Context, object types.Object) (state bulker.State, processedObject types.Object, err error) {
	defer func() {
		err = ps.postConsume(err)
		state = ps.state
	}()
	if err = ps.init(ctx); err != nil {
		return
	}

	//type mapping, flattening => table schema
	tableForObject, processedObject, err := ps.preprocess(object)
	if err != nil {
		return
	}
	batchFile := ps.batchFile != nil
	if batchFile {
		err = ps.writeToBatchFile(ctx, tableForObject, processedObject)
	} else {
		err = ps.insert(ctx, tableForObject, processedObject)
	}
	return
}

func (ps *AbstractTransactionalSQLStream) Abort(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	if ps.tx != nil {
		if ps.tmpTable != nil {
			_ = ps.tx.Drop(ctx, ps.tmpTable, true)
		}
		_ = ps.tx.Rollback()
	}
	if ps.batchFile != nil {
		_ = ps.batchFile.Close()
		_ = os.Remove(ps.batchFile.Name())
	}
	ps.state.Status = bulker.Aborted
	return ps.state, err
}

func (ps *AbstractTransactionalSQLStream) getPKValue(object types.Object) (string, error) {
	pkColumns := ps.pkColumns
	l := len(pkColumns)
	if l == 0 {
		return "", fmt.Errorf("primary key is not set")
	}
	if l == 1 {
		pkValue := object.GetN(ps.sqlAdapter.ColumnName(pkColumns[0]))
		return fmt.Sprint(pkValue), nil
	}
	pkArr := make([]string, 0, l)
	for _, col := range pkColumns {
		pkValue := object.GetN(ps.sqlAdapter.ColumnName(col))
		pkArr = append(pkArr, fmt.Sprint(pkValue))
	}
	return strings.Join(pkArr, "_###_"), nil
}
