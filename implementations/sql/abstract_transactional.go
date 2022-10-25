package sql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/implementations"
	"github.com/jitsucom/bulker/types"
	"os"
	"path"
	"strings"
	"time"
)

type AbstractTransactionalSQLStream struct {
	AbstractSQLStream
	tx       *TxSQLAdapter
	tmpTable *Table
	//function that generate tmp table schema based on target table schema
	tmpTableFunc       func(ctx context.Context, tableForObject *Table, batchFile bool) *Table
	dstTable           *Table
	batchFile          *os.File
	marshaller         types.Marshaller
	targetMarshaller   types.Marshaller
	eventsInBatch      int
	s3                 *implementations.S3
	batchFileLinesByPK map[string]int
	batchFileSkipLines utils.Set[int]
}

func newAbstractTransactionalStream(id string, p SQLAdapter, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (AbstractTransactionalSQLStream, error) {
	ps := AbstractTransactionalSQLStream{}
	abs, err := newAbstractStream(id, p, tableName, mode, streamOptions...)
	if err != nil {
		return ps, err
	}
	ps.AbstractSQLStream = abs
	if ps.merge {
		ps.batchFileLinesByPK = make(map[string]int)
		ps.batchFileSkipLines = utils.NewSet[int]()
	}
	return ps, nil
}

func (ps *AbstractTransactionalSQLStream) init(ctx context.Context) (err error) {
	if ps.inited {
		return nil
	}
	s3 := s3BatchFileOption.Get(&ps.options)
	if s3 != nil {
		s3Config := implementations.S3Config{AccessKeyID: s3.AccessKeyID, SecretKey: s3.SecretKey, Bucket: s3.Bucket, Region: s3.Region,
			FileConfig: implementations.FileConfig{Format: ps.sqlAdapter.GetBatchFileFormat()}}
		ps.s3, err = implementations.NewS3(&s3Config)
		if err != nil {
			return fmt.Errorf("failed to setup s3 client: %w", err)
		}
	}
	localBatchFile := localBatchFileOption.Get(&ps.options)
	if localBatchFile != "" && ps.batchFile == nil {
		ps.batchFile, err = os.CreateTemp("", localBatchFile)
		if err != nil {
			return err
		}
		ps.marshaller = &types.JSONMarshaller{}
		switch ps.sqlAdapter.GetBatchFileFormat() {
		case implementations.CSV:
			ps.targetMarshaller = &types.CSVMarshaller{}
		case implementations.CSV_GZIP:
			ps.targetMarshaller = &types.CSVMarshaller{Gzip: true}
		default:
			ps.targetMarshaller = &types.JSONMarshaller{}
		}
	}
	if ps.tx == nil {
		ps.tx, err = ps.sqlAdapter.OpenTx(ctx)
		if err != nil {
			return err
		}
		//set transactional adapter so all table modification will be performed inside transaction
		ps.tableHelper.SetSQLAdapter(ps.tx)
	}
	err = ps.AbstractSQLStream.init(ctx)
	if err != nil {
		return err
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
				_ = ps.tx.DropTable(ctx, ps.tmpTable.Name, true)
			}
			_ = ps.tx.Rollback()
		}
	} else {
		if ps.tx != nil {
			if ps.tmpTable != nil {
				_ = ps.tx.DropTable(ctx, ps.tmpTable.Name, true)
			}
			err = ps.tx.Commit()
		}
	}

	return ps.AbstractSQLStream.postComplete(err)
}

func (ps *AbstractTransactionalSQLStream) flushBatchFile(ctx context.Context) (err error) {
	table := ps.tmpTable
	err = ps.tx.CreateTable(ctx, table)
	if err != nil {
		return errorj.Decorate(err, "failed to create table")
	}
	columns := table.SortedColumnNames()
	defer func() {
		if ps.merge {
			ps.batchFileLinesByPK = make(map[string]int)
			ps.batchFileSkipLines = utils.NewSet[int]()
		}
		_ = ps.batchFile.Close()
		_ = os.Remove(ps.batchFile.Name())
	}()
	if ps.eventsInBatch > 0 {
		ps.marshaller.Flush()
		ps.batchFile.Sync()
		workingFile := ps.batchFile
		needToConvert := false
		convertStart := time.Now()
		if ps.targetMarshaller.Format() != ps.marshaller.Format() {
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
				err = ps.targetMarshaller.Init(workingFile, columns)
				if err != nil {
					return errorj.Decorate(err, "failed to write header for converted batch file")
				}
			}
			file, err := os.Open(ps.batchFile.Name())
			if err != nil {
				return errorj.Decorate(err, "failed to open tmp file")
			}
			scanner := bufio.NewScanner(file)
			i := 0
			for scanner.Scan() {
				if !ps.batchFileSkipLines.Contains(i) {
					if needToConvert {
						dec := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
						dec.UseNumber()
						obj := make(map[string]any)
						err = dec.Decode(&obj)
						if err != nil {
							return errorj.Decorate(err, "failed to decode json object from batch filer")
						}
						ps.targetMarshaller.Marshal(obj)
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
			ps.targetMarshaller.Flush()
			workingFile.Sync()
		}
		if needToConvert {
			logging.Infof("[%s] Converted batch file from %s to %s in %s", ps.id, ps.marshaller.Format(), ps.targetMarshaller.Format(), time.Now().Sub(convertStart))
		}
		if ps.s3 != nil {
			s3Config := s3BatchFileOption.Get(&ps.options)
			rFile, err := os.Open(workingFile.Name())
			if err != nil {
				return errorj.Decorate(err, "failed to open tmp file")
			}
			s3FileName := path.Base(workingFile.Name())
			if s3Config.Folder != "" {
				s3FileName = s3Config.Folder + "/" + s3FileName
			}
			err = ps.s3.Upload(s3FileName, rFile)
			if err != nil {
				return errorj.Decorate(err, "failed to upload file to s3")
			}
			defer ps.s3.DeleteObject(s3FileName)
			err = ps.tx.LoadTable(ctx, table, &LoadSource{Type: AmazonS3, Path: s3FileName, Format: ps.sqlAdapter.GetBatchFileFormat(), S3Config: s3Config})
			if err != nil {
				return errorj.Decorate(err, "failed to flush tmp file to the warehouse")
			}
		} else {
			err = ps.tx.LoadTable(ctx, table, &LoadSource{Type: LocalFile, Path: workingFile.Name(), Format: ps.sqlAdapter.GetBatchFileFormat()})
			if err != nil {
				return errorj.Decorate(err, "failed to flush tmp file to the warehouse")
			}
		}
	}
	return nil
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
//			err = ps.marshaller.WriteHeader((*targetTable).SortedColumnNames(), ps.batchFile)
//			if err != nil {
//				return errorj.Decorate(err, "failed write csv header")
//			}
//		}
//	}
//	return nil
//}

func (ps *AbstractTransactionalSQLStream) writeToBatchFile(ctx context.Context, targetTable *Table, processedObjects []types.Object) error {
	if ps.tmpTable == nil {
		ps.dstTable = targetTable
		ps.tmpTable = ps.tmpTableFunc(ctx, targetTable, true)
	} else {
		ps.tmpTable.Columns = utils.MapPutAll(targetTable.Columns, ps.tmpTable.Columns)
	}
	ps.marshaller.Init(ps.batchFile, targetTable.SortedColumnNames())
	for _, obj := range processedObjects {
		if ps.merge {
			pk, err := ps.getPKValue(obj)
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
		err := ps.marshaller.Marshal(obj)
		if err != nil {
			return errorj.Decorate(err, "failed to marshall into csv file")
		}
		ps.eventsInBatch++
	}
	return nil
}

func (ps *AbstractTransactionalSQLStream) insert(ctx context.Context, targetTable *Table, processedObjects []types.Object) (err error) {
	if ps.tmpTable == nil {
		ps.dstTable = targetTable
		ps.tmpTable = ps.tmpTableFunc(ctx, targetTable, false)
	} else {
		ps.tmpTable.Columns = utils.MapPutAll(targetTable.Columns, ps.tmpTable.Columns)
	}
	ps.tmpTable, err = ps.tableHelper.EnsureTableWithCaching(ctx, ps.id, ps.tmpTable)
	if err != nil {
		return errorj.Decorate(err, "failed to ensure table")
	}
	return ps.tx.Insert(ctx, ps.tmpTable, ps.merge, processedObjects)
}

func (ps *AbstractTransactionalSQLStream) Abort(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	if ps.tx != nil {
		_ = ps.tx.DropTable(ctx, ps.tmpTable.Name, true)
		_ = ps.tx.Rollback()
	}
	if ps.batchFile != nil {
		_ = ps.batchFile.Close()
		_ = os.Remove(ps.batchFile.Name())
	}
	ps.state.Status = bulker.Aborted
	return ps.state, err
}

func (ps *AbstractSQLStream) getPKValue(object types.Object) (string, error) {
	pkColumns := PrimaryKeyOption.Get(&ps.options)
	l := len(pkColumns)
	if l == 0 {
		return "", fmt.Errorf("primary key is not set")
	}
	if l == 1 {
		for col := range pkColumns {
			pkValue, ok := object[col]
			if !ok {
				return "", fmt.Errorf("primary key [%s] is not found in the object", col)
			}
			return fmt.Sprint(pkValue), nil
		}
	}
	var builder strings.Builder
	for col := range pkColumns {
		pkValue, ok := object[col]
		if ok {
			builder.WriteString(fmt.Sprint(pkValue))
			builder.WriteString("_")
		}
	}
	if builder.Len() > 0 {
		return builder.String(), nil
	}
	return "", fmt.Errorf("primary key columns not found in the object")
}
