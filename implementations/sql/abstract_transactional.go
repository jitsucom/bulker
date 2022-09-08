package sql

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/implementations"
	"github.com/jitsucom/bulker/types"
	"os"
	"path"
	"strings"
)

type AbstractTransactionalSQLStream struct {
	AbstractSQLStream
	tx               *TxSQLAdapter
	tmpTable         *Table
	dstTable         *Table
	tmpFile          *os.File
	tmpWritten       int
	s3               *implementations.S3
	tmpFileLineByPK  map[string]int
	tmpFileSkipLines utils.Set[int]
}

func newAbstractTransactionalStream(id string, p SQLAdapter, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (AbstractTransactionalSQLStream, error) {
	ps := AbstractTransactionalSQLStream{}
	abs, err := newAbstractStream(id, p, tableName, mode, streamOptions...)
	if err != nil {
		return ps, err
	}
	ps.AbstractSQLStream = abs
	if ps.merge {
		ps.tmpFileLineByPK = make(map[string]int)
		ps.tmpFileSkipLines = utils.NewSet[int]()
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
			FileConfig: implementations.FileConfig{Format: implementations.FileFormatCSV}}
		ps.s3, err = implementations.NewS3(&s3Config)
		if err != nil {
			return fmt.Errorf("failed to setup s3 client: %w", err)
		}
	}
	localBatchFile := localBatchFileOption.Get(&ps.options)
	if localBatchFile != "" && ps.tmpFile == nil {
		ps.tmpFile, err = os.CreateTemp("", localBatchFile)
		if err != nil {
			return err
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
	if ps.tmpFile != nil {
		_ = ps.tmpFile.Close()
		_ = os.Remove(ps.tmpFile.Name())
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

func (ps *AbstractTransactionalSQLStream) flushTmpFile(ctx context.Context, table *Table, deleteFile bool) (err error) {
	defer func() {
		if ps.merge {
			ps.tmpFileLineByPK = make(map[string]int)
			ps.tmpFileSkipLines = utils.NewSet[int]()
		}
		if deleteFile {
			_ = ps.tmpFile.Close()
			_ = os.Remove(ps.tmpFile.Name())
		} else {
			ps.tmpWritten = 0
			ps.tmpFile.Seek(0, 0)
			err1 := ps.tmpFile.Truncate(0)
			if err1 != nil {
				err = multierror.Append(err, errorj.Decorate(err1, "failed truncate csv file"))
			}
		}
	}()
	if ps.tmpWritten > 0 {
		ps.tmpFile.Sync()
		workingFile := ps.tmpFile
		if len(ps.tmpFileSkipLines) > 0 {
			workingFile, err = os.CreateTemp("", path.Base(ps.tmpFile.Name())+"_dedupl")
			if err != nil {
				return errorj.Decorate(err, "failed to create tmp file for deduplication")
			}
			defer func() {
				_ = workingFile.Close()
				_ = os.Remove(workingFile.Name())
			}()
			file, err := os.Open(ps.tmpFile.Name())
			if err != nil {
				return errorj.Decorate(err, "failed to open tmp file")
			}
			scanner := bufio.NewScanner(file)
			i := 0
			for scanner.Scan() {
				if !ps.tmpFileSkipLines.Contains(i) {
					_, err := workingFile.Write(scanner.Bytes())
					if err != nil {
						return errorj.Decorate(err, "failed write to deduplication file")
					}
					_, _ = workingFile.Write([]byte("\n"))
				}
				i++
			}
			workingFile.Sync()
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
			err = ps.tx.LoadTable(ctx, table, &LoadSource{Type: AmazonS3, Path: s3FileName, Format: CSV, S3Config: s3Config})
			if err != nil {
				return errorj.Decorate(err, "failed to flush tmp file to the warehouse")
			}
		} else {
			err = ps.tx.LoadTable(ctx, table, &LoadSource{Type: LocalFile, Path: workingFile.Name(), Format: CSV})
			if err != nil {
				return errorj.Decorate(err, "failed to flush tmp file to the warehouse")
			}
		}
	}
	return nil
}

func (ps *AbstractTransactionalSQLStream) ensureSchema(ctx context.Context, targetTable **Table, tableForObject *Table, initTable func(ctx context.Context) (*Table, error)) (err error) {
	needRenewTmpTable := false
	//first object
	if *targetTable == nil {
		*targetTable, err = initTable(ctx)
		if err != nil {
			return err
		}
		needRenewTmpTable = true
	} else {
		if !tableForObject.FitsToTable(*targetTable) {
			needRenewTmpTable = true
			if ps.tmpFile != nil {
				logging.Infof("[%s] Table schema changed during transaction. New columns: %v", ps.id, tableForObject.Diff(*targetTable).Columns)
				if err = ps.flushTmpFile(ctx, *targetTable, false); err != nil {
					return err
				}
			}
			(*targetTable).Columns = utils.MapPutAll(tableForObject.Columns, (*targetTable).Columns)
		}
	}
	if needRenewTmpTable {
		//adapt tmp table for new object columns if any
		*targetTable, err = ps.tableHelper.EnsureTableWithCaching(ctx, ps.id, *targetTable)
		if err != nil {
			return errorj.Decorate(err, "failed to ensure temporary table")
		}
		if ps.tmpFile != nil {
			err = types.CSVMarshallerInstance.WriteHeader((*targetTable).SortedColumnNames(), ps.tmpFile)
			if err != nil {
				return errorj.Decorate(err, "failed write csv header")
			}
		}
	}
	return nil
}

func (ps *AbstractTransactionalSQLStream) insert(ctx context.Context, targetTable *Table, processedObjects []types.Object) error {
	if ps.tmpFile != nil {
		for _, obj := range processedObjects {
			if ps.merge {
				pk, err := ps.getPKValue(obj)
				if err != nil {
					return err
				}
				line, ok := ps.tmpFileLineByPK[pk]
				if ok {
					ps.tmpFileSkipLines.Put(line)
				}
				ps.tmpFileLineByPK[pk] = ps.tmpWritten + 1 //+1 for header
			}
			err := types.CSVMarshallerInstance.Marshal(targetTable.SortedColumnNames(), ps.tmpFile, obj)
			if err != nil {
				return errorj.Decorate(err, "failed to marshall into csv file")
			}
			ps.tmpWritten++
		}
		return nil
	} else {
		return ps.tx.Insert(ctx, targetTable, ps.merge, processedObjects)
	}
}

func (ps *AbstractTransactionalSQLStream) Abort(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	if ps.tx != nil {
		_ = ps.tx.DropTable(ctx, ps.tmpTable.Name, true)
		_ = ps.tx.Rollback()
	}
	if ps.tmpFile != nil {
		_ = ps.tmpFile.Close()
		_ = os.Remove(ps.tmpFile.Name())
	}
	ps.state.Status = bulker.Aborted
	return ps.state, err
}

func (ps *AbstractSQLStream) getPKValue(object types.Object) (string, error) {
	pkColumns := primaryKeyOption.Get(&ps.options)
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
