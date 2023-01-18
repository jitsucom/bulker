package sql

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/implementations"
	"github.com/jitsucom/bulker/types"
	jsoniter "github.com/json-iterator/go"
	"os"
	"path"
	"strings"
	"time"
)

const unmappedDataColumn = "_unmapped_data"

type AbstractTransactionalSQLStream struct {
	AbstractSQLStream
	tx       *TxSQLAdapter
	tmpTable *Table
	//function that generate tmp table schema based on target table schema
	tmpTableFunc       func(ctx context.Context, tableForObject *Table, object types.Object) (table *Table)
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
	err = ps.AbstractSQLStream.init(ctx)
	if err != nil {
		return err
	}
	if ps.tx == nil {
		ps.tx, err = ps.sqlAdapter.OpenTx(ctx)
		if err != nil {
			return err
		}
		//set transactional adapter so all table modification will be performed inside transaction
		ps.tableHelper.SetSQLAdapter(ps.tx)
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
	existingTable, _ := ps.tx.GetTableSchema(ctx, ps.tableName)
	if existingTable.Exists() {
		//we need to respect types of existing columns when we create tmp table
		ps.tmpTable.Columns = utils.MapPutAll(ps.tmpTable.Columns, existingTable.Columns)
	}
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
						dec := jsoniter.NewDecoder(bytes.NewReader(scanner.Bytes()))
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

func (ps *AbstractTransactionalSQLStream) writeToBatchFile(ctx context.Context, targetTable *Table, processedObject types.Object) error {
	ps.adjustTables(ctx, targetTable, processedObject)
	ps.updateRepresentationTable(ps.tmpTable)
	ps.marshaller.Init(ps.batchFile, targetTable.SortedColumnNames())
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

func (ps *AbstractTransactionalSQLStream) insert(ctx context.Context, targetTable *Table, processedObject types.Object) (err error) {
	ps.adjustTables(ctx, targetTable, processedObject)
	ps.updateRepresentationTable(ps.tmpTable)
	ps.tmpTable, err = ps.tableHelper.EnsureTableWithoutCaching(ctx, ps.id, ps.tmpTable)
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
		ps.adjustTableColumnTypes(ps.tmpTable, targetTable, processedObject)
	}
	ps.dstTable.Columns = ps.tmpTable.Columns
}

// adjustTableColumnTypes modify existingTable with extra new columns from desiredTable if such exists
// if some column already exists in the database, no problems if its DataType is castable to DataType of existing column
// if some new column is being added but with different DataTypes - type of this column will be changed to a common ancestor type
// object values that can't be casted will be added to '_unmaped_data' column of JSON type as an json object
func (ps *AbstractTransactionalSQLStream) adjustTableColumnTypes(existingTable, desiredTable *Table, values types.Object) {
	cloned := existingTable.Columns.Clone()
	unmappedObj := map[string]any{}
	for name, newCol := range desiredTable.Columns {
		existingCol, ok := cloned[name]
		if !ok {
			//column not exist in database - adding as New
			newCol.New = true
			cloned[name] = newCol
			continue
		}
		if newCol.Override {
			//if column sql type is overridden by user - leave it this way
			cloned[name] = newCol
			continue
		}
		if existingCol.DataType == newCol.DataType {
			continue
		}
		if !existingCol.New {
			//column exists in database - check if its DataType is castable to DataType of existing column
			if types.IsConvertible(newCol.DataType, existingCol.DataType) {
				newVal, err := types.Convert(existingCol.DataType, values[name])
				if err != nil {
					//logging.Warnf("Can't convert '%s' value '%v' from %s to %s: %v", name, values[name], newCol.DataType.String(), existingCol.DataType.String(), err)
					unmappedObj[name] = values[name]
					delete(values, name)
					continue
				} else {
					//logging.Infof("Converted '%s' value '%v' from %s to %s: %v", name, values[name], newCol.DataType.String(), existingCol.DataType.String(), newVal)
					values[name] = newVal
				}
			} else {
				//logging.Warnf("Can't convert '%s' value '%v' from %s to %s", name, values[name], newCol.DataType.String(), existingCol.DataType.String())
				unmappedObj[name] = values[name]
				delete(values, name)
				continue
			}

		} else {
			common := types.GetCommonAncestorType(existingCol.DataType, newCol.DataType)
			if common != existingCol.DataType {
				//logging.Warnf("Changed '%s' type from %s to %s because of %s", name, existingCol.DataType.String(), common.String(), newCol.DataType.String())
				sqlType, ok := ps.sqlAdapter.GetSQLType(common)
				if ok {
					existingCol.DataType = common
					existingCol.Type = sqlType
					cloned[name] = existingCol
				} else {
					logging.SystemErrorf("Unknown column type %s mapping for %s", common, ps.sqlAdapter.Type())
				}
			}

		}
	}
	if len(unmappedObj) > 0 {
		jsonSQLType, _ := ps.sqlAdapter.GetSQLType(types.JSON)
		utils.MapPutIfAbsent(cloned, unmappedDataColumn, SQLColumn{DataType: types.JSON, Type: jsonSQLType})
		b, _ := jsoniter.Marshal(unmappedObj)
		values[unmappedDataColumn] = string(b)
	}
	existingTable.Columns = cloned
}

func (ps *AbstractTransactionalSQLStream) Consume(ctx context.Context, object types.Object) (state bulker.State, processedObjects []types.Object, err error) {
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
		ps.updateRepresentationTable(tableForObject)
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
			_ = ps.tx.DropTable(ctx, ps.tmpTable.Name, true)
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
	l := len(ps.pkColumns)
	if l == 0 {
		return "", fmt.Errorf("primary key is not set")
	}
	if l == 1 {
		for col := range ps.pkColumns {
			pkValue, ok := object[col]
			if !ok {
				return "", fmt.Errorf("primary key [%s] is not found in the object", col)
			}
			return fmt.Sprint(pkValue), nil
		}
	}
	var builder strings.Builder
	for col := range ps.pkColumns {
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
