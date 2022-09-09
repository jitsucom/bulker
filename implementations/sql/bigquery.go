package sql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/implementations"
	"github.com/jitsucom/bulker/types"
	"google.golang.org/api/iterator"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

//TODO: no primary key support

func init() {
	bulker.RegisterBulker(BigqueryBulkerTypeId, NewBigquery)
}

const (
	BigQueryAutocommitUnsupported = "BigQuery bulker doesn't support auto commit mode as not efficient"
	BigqueryBulkerTypeId          = "bigquery"

	mergeBigQueryTemplate  = "MERGE INTO `%s` T USING `%s` S ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)"
	deleteBigQueryTemplate = "DELETE FROM `%s` WHERE %s"
	updateBigQueryTemplate = "UPDATE `%s` SET %s WHERE %s"

	truncateBigQueryTemplate = "TRUNCATE TABLE `%s`"
	selectBigQueryTemplate   = "SELECT %s FROM `%s`%s%s"

	//createIndexBigQueryTemplate = "CREATE SEARCH INDEX %s ON `%s`(%s) OPTIONS (analyzer = 'NO_OP_ANALYZER')"
	//dropIndexBigQueryTemplate   = "DROP SEARCH INDEX %s ON `%s`"

	rowsLimitPerInsertOperation = 500
)

var (
	//SchemaToBigQueryString is mapping between JSON types and BigQuery types
	SchemaToBigQueryString = map[types.DataType]string{
		types.STRING:    string(bigquery.StringFieldType),
		types.INT64:     string(bigquery.IntegerFieldType),
		types.FLOAT64:   string(bigquery.FloatFieldType),
		types.TIMESTAMP: string(bigquery.TimestampFieldType),
		types.BOOL:      string(bigquery.BooleanFieldType),
		types.UNKNOWN:   string(bigquery.StringFieldType),
	}
)

// BigQuery adapter for creating,patching (schema or table), inserting and copying data from gcs to BigQuery
type BigQuery struct {
	client      *bigquery.Client
	config      *implementations.GoogleConfig
	queryLogger *logging.QueryLogger
}

// NewBigquery return configured BigQuery bulker.Bulker instance
func NewBigquery(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &implementations.GoogleConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %w", err)
	}
	var err error
	err = config.Validate()
	if err != nil {
		return nil, fmt.Errorf("failed to validate config: %w", err)
	}
	var client *bigquery.Client
	ctx := context.Background()
	if config.Credentials == nil {
		client, err = bigquery.NewClient(ctx, config.Project)
	} else {
		client, err = bigquery.NewClient(ctx, config.Project, config.Credentials)
	}

	if err != nil {
		return nil, fmt.Errorf("Error creating BigQuery client: %v", err)
	}

	return &BigQuery{client: client, config: config, queryLogger: logging.NewQueryLogger(bulkerConfig.Id, os.Stderr, os.Stderr)}, nil
}

func (bq *BigQuery) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	bq.validateOptions(streamOptions)
	streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s_stream_%s_%s", mode, tableName, utils.SanitizeString(id))))
	switch mode {
	case bulker.AutoCommit:
		return nil, errors.New(BigQueryAutocommitUnsupported)
		//return newAutoCommitStream(id, bq, tableName, streamOptions...)
	case bulker.Transactional:
		return newTransactionalStream(id, bq, tableName, streamOptions...)
	case bulker.ReplaceTable:
		return newReplaceTableStream(id, bq, tableName, streamOptions...)
	case bulker.ReplacePartition:
		return newReplacePartitionStream(id, bq, tableName, streamOptions...)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}

func (bq *BigQuery) validateOptions(streamOptions []bulker.StreamOption) error {
	options := &bulker.StreamOptions{}
	for _, option := range streamOptions {
		option(options)
	}
	return nil
}

func (bq *BigQuery) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, merge bool) (err error) {
	if !merge {
		defer func() {
			if err != nil {
				err = errorj.CopyError.Wrap(err, "failed to run BQ copier").
					WithProperty(errorj.DBInfo, &types.ErrorPayload{
						Dataset: bq.config.Dataset,
						Bucket:  bq.config.Bucket,
						Project: bq.config.Project,
						Table:   targetTable.Name,
					})
			}
		}()
		dataset := bq.client.Dataset(bq.config.Dataset)
		copier := dataset.Table(targetTable.Name).CopierFrom(dataset.Table(sourceTable.Name))
		copier.WriteDisposition = bigquery.WriteAppend
		copier.CreateDisposition = bigquery.CreateIfNeeded
		bq.logQuery(fmt.Sprintf("Copy tables values to table %s from: %s ", targetTable.Name, sourceTable.Name), nil, err)
		job, err := copier.Run(ctx)
		if err != nil {
			return err
		}
		jobStatus, err := job.Wait(ctx)
		if err != nil {
			return err
		}
		if jobStatus.Err() != nil {
			return jobStatus.Err()
		}

		return nil
	} else {
		defer func() {
			if err != nil {
				err = errorj.BulkMergeError.Wrap(err, "failed to run merge").
					WithProperty(errorj.DBInfo, &types.ErrorPayload{
						Dataset: bq.config.Dataset,
						Bucket:  bq.config.Bucket,
						Project: bq.config.Project,
						Table:   targetTable.Name,
					})
			}
		}()

		columns := sourceTable.SortedColumnNames()
		var updateSet []string
		for _, name := range columns {
			updateSet = append(updateSet, fmt.Sprintf("T.%s = S.%s", name, name))
		}
		var joinConditions []string
		for pkField := range targetTable.PKFields {
			joinConditions = append(joinConditions, fmt.Sprintf("T.%s = S.%s", pkField, pkField))
		}
		columnsString := strings.Join(columns, ",")
		insertFromSelectStatement := fmt.Sprintf(mergeBigQueryTemplate, bq.fullTableName(targetTable.Name), bq.fullTableName(sourceTable.Name),
			strings.Join(joinConditions, " AND "), strings.Join(updateSet, ", "), columnsString, columnsString)

		query := bq.client.Query(insertFromSelectStatement)
		job, err := query.Run(ctx)
		bq.logQuery(insertFromSelectStatement, nil, err)
		if err != nil {
			return err
		}
		status, err := job.Wait(ctx)
		if err != nil {
			return err
		}
		return status.Err()
	}
}

// Copy transfers data from google cloud storage file to google BigQuery table as one batch
func (bq *BigQuery) Copy(ctx context.Context, fileKey, tableName string) error {
	table := bq.client.Dataset(bq.config.Dataset).Table(tableName)

	gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%s/%s", bq.config.Bucket, fileKey))
	gcsRef.SourceFormat = bigquery.JSON
	loader := table.LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateNever

	job, err := loader.Run(ctx)
	if err != nil {
		return errorj.CopyError.Wrap(err, "failed to run BQ loader").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Dataset: bq.config.Dataset,
				Bucket:  bq.config.Bucket,
				Project: bq.config.Project,
				Table:   tableName,
			})
	}
	jobStatus, err := job.Wait(ctx)
	if err != nil {
		return errorj.CopyError.Wrap(err, "failed to wait BQ job").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Dataset: bq.config.Dataset,
				Bucket:  bq.config.Bucket,
				Project: bq.config.Project,
				Table:   tableName,
			})
	}

	if jobStatus.Err() != nil {
		return errorj.CopyError.Wrap(jobStatus.Err(), "failed due to BQ job status").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Dataset: bq.config.Dataset,
				Bucket:  bq.config.Bucket,
				Project: bq.config.Project,
				Table:   tableName,
			})
	}

	return nil
}

func (bq *BigQuery) Test() error {
	_, err := bq.client.Query("SELECT 1;").Read(context.Background())
	return err
}

func (bq *BigQuery) GetTypesMapping() map[types.DataType]string {
	return SchemaToBigQueryString
}

// GetTableSchema return google BigQuery table (name,columns) representation wrapped in Table struct
func (bq *BigQuery) GetTableSchema(ctx context.Context, tableName string) (*Table, error) {
	table := &Table{Name: tableName, Columns: Columns{}, PKFields: utils.Set[string]{}}

	bqTable := bq.client.Dataset(bq.config.Dataset).Table(tableName)

	meta, err := bqTable.Metadata(ctx)
	if err != nil {
		if isNotFoundErr(err) {
			return table, nil
		}

		return nil, errorj.GetTableError.Wrap(err, "failed to get table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Dataset: bq.config.Dataset,
				Bucket:  bq.config.Bucket,
				Project: bq.config.Project,
				Table:   tableName,
			})
	}
	for _, field := range meta.Schema {
		table.Columns[field.Name] = SQLColumn{Type: string(field.Type)}
	}
	pkFieldName := BuildConstraintName(table.Name)
	pkFieldsString, ok := meta.Labels[pkFieldName]
	if ok {
		pkFields := strings.Split(pkFieldsString, ",")
		for _, pkField := range pkFields {
			table.PKFields.Put(pkField)
		}
		table.PrimaryKeyName = pkFieldName
	}
	////Load managed indexes
	//indexName := BuildConstraintName(table.Name)
	//searchIndexTable := "INFORMATION_SCHEMA.SEARCH_INDEX_COLUMNS"
	//whenConditions := (&WhenConditions{}).Add("index_catalog", "=", bq.config.Project).
	//	Add("index_schema", "=", bq.config.Dataset).
	//	Add("table_name", "=", table.Name).
	//	Add("index_name", "=", indexName)
	//data, err := bq.selectFrom(ctx, searchIndexTable, "index_column_name", whenConditions, "")
	//if err != nil {
	//	return nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to get indexes information").
	//		WithProperty(errorj.DBInfo, &types.ErrorPayload{
	//			Dataset: bq.config.Dataset,
	//			Bucket:  bq.config.Bucket,
	//			Project: bq.config.Project,
	//			Table:   tableName,
	//		})
	//}
	//if len(data) >= 0 {
	//	table.PrimaryKeyName = indexName
	//	pkFields := utils.NewSet[string]()
	//	for _, row := range data {
	//		col := row["index_column_name"]
	//		pkFields.Put(fmt.Sprint(col))
	//	}
	//	table.PKFields = pkFields
	//}
	return table, nil
}

// CreateTable creates google BigQuery table from Table
func (bq *BigQuery) CreateTable(ctx context.Context, table *Table) (err error) {
	bqTable := bq.client.Dataset(bq.config.Dataset).Table(table.Name)

	_, err = bqTable.Metadata(ctx)
	if err == nil {
		logging.Info("BigQuery table", table.Name, "already exists")
		return nil
	}

	if !isNotFoundErr(err) {
		return errorj.GetTableError.Wrap(err, "failed to get table metadata").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Dataset: bq.config.Dataset,
				Bucket:  bq.config.Bucket,
				Project: bq.config.Project,
				Table:   table.Name,
			})
	}

	bqSchema := bigquery.Schema{}
	for _, columnName := range table.SortedColumnNames() {
		column := table.Columns[columnName]
		bigQueryType := bigquery.FieldType(strings.ToUpper(column.GetDDLType()))
		bqSchema = append(bqSchema, &bigquery.FieldSchema{Name: columnName, Type: bigQueryType})
	}
	var labels map[string]string
	if len(table.PKFields) > 0 && table.PrimaryKeyName != "" {
		labels = map[string]string{table.PrimaryKeyName: strings.Join(table.PKFields.ToSlice(), ",")}
	}
	tableMetaData := bigquery.TableMetadata{Name: table.Name, Schema: bqSchema, Labels: labels}
	bq.logQuery("CREATE table for schema: ", tableMetaData, nil)
	if table.Partition.Field != "" && table.Partition.Granularity != ALL {
		var partitioningType bigquery.TimePartitioningType
		switch table.Partition.Granularity {
		case DAY:
		case WEEK:
			partitioningType = bigquery.DayPartitioningType
		case MONTH:
		case QUARTER:
			partitioningType = bigquery.MonthPartitioningType
		case YEAR:
			partitioningType = bigquery.YearPartitioningType
		}
		tableMetaData.TimePartitioning = &bigquery.TimePartitioning{Field: table.Partition.Field, Type: partitioningType}
	}
	if err := bqTable.Create(ctx, &tableMetaData); err != nil {
		schemaJson, _ := bqSchema.ToJSONFields()
		return errorj.GetTableError.Wrap(err, "failed to create table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Dataset:   bq.config.Dataset,
				Bucket:    bq.config.Bucket,
				Project:   bq.config.Project,
				Table:     table.Name,
				Statement: string(schemaJson),
			})
	}

	return nil
}

// InitDatabase creates google BigQuery Dataset if doesn't exist
func (bq *BigQuery) InitDatabase(ctx context.Context) error {
	dataset := bq.config.Dataset
	bqDataset := bq.client.Dataset(dataset)
	if _, err := bqDataset.Metadata(ctx); err != nil {
		if isNotFoundErr(err) {
			datasetMetadata := &bigquery.DatasetMetadata{Name: dataset}
			bq.logQuery("CREATE dataset: ", datasetMetadata, nil)
			if err := bqDataset.Create(ctx, datasetMetadata); err != nil {
				return errorj.CreateSchemaError.Wrap(err, "failed to create dataset").
					WithProperty(errorj.DBInfo, &types.ErrorPayload{
						Dataset: dataset,
					})
			}
		} else {
			return errorj.CreateSchemaError.Wrap(err, "failed to get dataset metadata").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Dataset: dataset,
				})
		}
	}

	return nil
}

// PatchTableSchema adds Table columns to google BigQuery table
func (bq *BigQuery) PatchTableSchema(ctx context.Context, patchSchema *Table) error {
	bqTable := bq.client.Dataset(bq.config.Dataset).Table(patchSchema.Name)
	metadata, err := bqTable.Metadata(ctx)
	if err != nil {
		return errorj.PatchTableError.Wrap(err, "failed to get table metadata").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Dataset: bq.config.Dataset,
				Bucket:  bq.config.Bucket,
				Project: bq.config.Project,
				Table:   patchSchema.Name,
			})
	}
	//patch primary keys - delete old
	if patchSchema.DeletePkFields {
		metadata.Labels = map[string]string{}
	}

	//patch primary keys - create new
	if len(patchSchema.PKFields) > 0 && patchSchema.PrimaryKeyName != "" {
		metadata.Labels = map[string]string{patchSchema.PrimaryKeyName: strings.Join(patchSchema.PKFields.ToSlice(), ",")}
	}
	for _, columnName := range patchSchema.SortedColumnNames() {
		column := patchSchema.Columns[columnName]
		bigQueryType := bigquery.FieldType(strings.ToUpper(column.GetDDLType()))
		metadata.Schema = append(metadata.Schema, &bigquery.FieldSchema{Name: columnName, Type: bigQueryType})
	}
	updateReq := bigquery.TableMetadataToUpdate{Schema: metadata.Schema}
	bq.logQuery("PATCH update request: ", updateReq, nil)
	if _, err := bqTable.Update(ctx, updateReq, metadata.ETag); err != nil {
		schemaJson, _ := metadata.Schema.ToJSONFields()
		return errorj.PatchTableError.Wrap(err, "failed to patch table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Dataset:   bq.config.Dataset,
				Bucket:    bq.config.Bucket,
				Project:   bq.config.Project,
				Table:     patchSchema.Name,
				Statement: string(schemaJson),
			})
	}

	return nil
}

func (bq *BigQuery) DeletePartition(ctx context.Context, tableName string, datePartiton *DatePartition) error {
	partitions := GranularityToPartitionIds(datePartiton.Granularity, datePartiton.Value)
	for _, partition := range partitions {
		bq.logQuery("DELETE partition "+partition+" in table"+tableName, "", nil)
		logging.Infof("Deletion partition %s in table %s", partition, tableName)
		if err := bq.client.Dataset(bq.config.Dataset).Table(tableName + "$" + partition).Delete(ctx); err != nil {
			gerr, ok := err.(*googleapi.Error)
			if ok && gerr.Code == 404 {
				logging.Infof("Partition %s$%s was not found", tableName, partition)
				continue
			}
			return errorj.DeleteFromTableError.Wrap(err, "failed to delete partition").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Dataset:   bq.config.Dataset,
					Bucket:    bq.config.Bucket,
					Project:   bq.config.Project,
					Table:     tableName,
					Partition: partition,
				})
		}

	}
	return nil
}

func GranularityToPartitionIds(g Granularity, t time.Time) []string {
	t = g.Lower(t)
	switch g {
	case HOUR:
		return []string{t.Format("2006010215")}
	case DAY:
		return []string{t.Format("20060102")}
	case WEEK:
		week := make([]string, 0, 7)
		for i := 0; i < 7; i++ {
			week = append(week, t.AddDate(0, 0, i).Format("20060102"))
		}
		return week
	case MONTH:
		return []string{t.Format("200601")}
	case QUARTER:
		quarter := make([]string, 0, 3)
		for i := 0; i < 3; i++ {
			quarter = append(quarter, t.AddDate(0, i, 0).Format("200601"))
		}
		return quarter
	case YEAR:
		return []string{t.Format("2006")}
	default:
		logging.SystemErrorf("Granularity %s is not mapped to any partition time unit:.", g)
		return []string{}
	}
}

// insertBatch streams data into BQ using stream API
// 1 insert = max 500 rows
func (bq *BigQuery) Insert(ctx context.Context, table *Table, merge bool, objects []types.Object) (err error) {
	inserter := bq.client.Dataset(bq.config.Dataset).Table(table.Name).Inserter()
	bq.logQuery(fmt.Sprintf("Inserting [%d] values to table %s using BigQuery Streaming API with chunks [%d]: ", len(objects), table.Name, rowsLimitPerInsertOperation), objects, nil)

	items := make([]*BQItem, 0, rowsLimitPerInsertOperation)
	operation := 0
	operations := int(math.Max(1, float64(len(objects))/float64(rowsLimitPerInsertOperation)))
	for _, object := range objects {
		if len(items) > rowsLimitPerInsertOperation {
			operation++
			if err := bq.insertItems(ctx, inserter, items); err != nil {
				return errorj.ExecuteInsertInBatchError.Wrap(err, "failed to execute middle insert %d of %d in batch", operation, operations).
					WithProperty(errorj.DBInfo, &types.ErrorPayload{
						Dataset: bq.config.Dataset,
						Bucket:  bq.config.Bucket,
						Project: bq.config.Project,
						Table:   table.Name,
					})
			}

			items = make([]*BQItem, 0, rowsLimitPerInsertOperation)
		}

		items = append(items, &BQItem{values: object})
	}

	if len(items) > 0 {
		operation++
		if err := bq.insertItems(ctx, inserter, items); err != nil {
			return errorj.ExecuteInsertInBatchError.Wrap(err, "failed to execute last insert %d of %d in batch", operation, operations).
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Dataset: bq.config.Dataset,
					Bucket:  bq.config.Bucket,
					Project: bq.config.Project,
					Table:   table.Name,
				})
		}
	}

	return nil
}

func (bq *BigQuery) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (err error) {
	defer func() {
		if err != nil {
			err = errorj.ExecuteInsertInBatchError.Wrap(err, "failed to execute middle insert batch").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Dataset: bq.config.Dataset,
					Bucket:  bq.config.Bucket,
					Project: bq.config.Project,
					Table:   targetTable.Name,
				})
		}
	}()
	bq.logQuery(fmt.Sprintf("Loading values to table %s from: %s ", targetTable.Name, loadSource.Path), nil, nil)
	//f, err := os.ReadFile(loadSource.Path)
	//logging.Infof("FILE: %s", f)

	file, err := os.Open(loadSource.Path)
	if err != nil {
		return err
	}
	bqTable := bq.client.Dataset(bq.config.Dataset).Table(targetTable.Name)
	meta, err := bqTable.Metadata(ctx)

	mp := make(map[string]*bigquery.FieldSchema, len(meta.Schema))
	for _, field := range meta.Schema {
		mp[field.Name] = field
	}
	for i, field := range targetTable.SortedColumnNames() {
		if _, ok := mp[field]; !ok {
			return fmt.Errorf("field %s is not in table schema", field)
		}
		meta.Schema[i] = mp[field]
	}
	//for _, field := range meta.Schema {
	//	logging.Infof("FIELD: %s", field.Name)
	//}
	source := bigquery.NewReaderSource(file)
	source.Schema = meta.Schema

	if loadSource.Format == CSV {
		source.SourceFormat = bigquery.CSV
		source.SkipLeadingRows = 1
		source.CSVOptions.NullMarker = "\\N"
	} else if loadSource.Format == JSON {
		source.SourceFormat = bigquery.JSON
	}
	loader := bq.client.Dataset(bq.config.Dataset).Table(targetTable.Name).LoaderFrom(source)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	loader.WriteDisposition = bigquery.WriteAppend
	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	return nil
}

// DropTable drops table from BigQuery
func (bq *BigQuery) DropTable(ctx context.Context, tableName string, ifExists bool) error {
	bq.logQuery(fmt.Sprintf("DROP table '%s' if exists: %t", tableName, ifExists), nil, nil)

	bqTable := bq.client.Dataset(bq.config.Dataset).Table(tableName)
	_, err := bqTable.Metadata(ctx)
	gerr, ok := err.(*googleapi.Error)
	if ok && gerr.Code == 404 && ifExists {
		return nil
	}
	if err := bqTable.Delete(ctx); err != nil {
		return errorj.DropError.Wrap(err, "failed to drop table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Dataset: bq.config.Dataset,
				Bucket:  bq.config.Bucket,
				Project: bq.config.Project,
				Table:   tableName,
			})
	}

	return nil
}

func (bq *BigQuery) ReplaceTable(ctx context.Context, originalTable, replacementTable string, dropOldTable bool) (err error) {
	defer func() {
		if err != nil {
			err = errorj.CopyError.Wrap(err, "failed to replace table").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Dataset: bq.config.Dataset,
					Bucket:  bq.config.Bucket,
					Project: bq.config.Project,
					Table:   originalTable,
				})
		}
	}()
	dataset := bq.client.Dataset(bq.config.Dataset)
	copier := dataset.Table(originalTable).CopierFrom(dataset.Table(replacementTable))
	copier.WriteDisposition = bigquery.WriteTruncate
	copier.CreateDisposition = bigquery.CreateIfNeeded
	bq.logQuery(fmt.Sprintf("COPY table '%s' to '%s'", replacementTable, originalTable), copier, nil)

	job, err := copier.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err

	}
	if err = status.Err(); err != nil {
		return err
	}
	if dropOldTable {
		return bq.DropTable(ctx, replacementTable, false)
	} else {
		return nil
	}
}

// TruncateTable deletes all records in tableName table
func (bq *BigQuery) TruncateTable(ctx context.Context, tableName string) error {
	query := fmt.Sprintf(truncateBigQueryTemplate, bq.fullTableName(tableName))
	bq.logQuery(query, nil, nil)
	if _, err := bq.client.Query(query).Read(ctx); err != nil {
		extraText := ""
		if strings.Contains(err.Error(), "Not found") {
			extraText = ": " + ErrTableNotExist.Error()
		}
		return errorj.TruncateError.Wrap(err, "failed to truncate table"+extraText).
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Dataset: bq.config.Dataset,
				Bucket:  bq.config.Bucket,
				Project: bq.config.Project,
				Table:   tableName,
			})
	}
	//TODO: temporary workaround for "404: Table is truncated" error until #958 is done
	time.Sleep(time.Minute)
	return nil
}

func (bq *BigQuery) insertItems(ctx context.Context, inserter *bigquery.Inserter, items []*BQItem) error {
	if err := inserter.Put(ctx, items); err != nil {
		var multiErr error
		if putMultiError, ok := err.(bigquery.PutMultiError); ok {
			for _, errUnit := range putMultiError {
				multiErr = multierror.Append(multiErr, errors.New(errUnit.Error()))
			}
		} else {
			multiErr = err
		}

		return multiErr
	}

	return nil
}

func (bq *BigQuery) toDeleteQuery(conditions *WhenConditions) string {
	var queryConditions []string

	for _, condition := range conditions.Conditions {
		conditionString := fmt.Sprintf("%v %v %q", condition.Field, condition.Clause, condition.Value)
		queryConditions = append(queryConditions, conditionString)
	}

	return strings.Join(queryConditions, " "+conditions.JoinCondition+" ")
}

func (bq *BigQuery) logQuery(messageTemplate string, entity interface{}, err error) {
	entityString := ""
	if entity != nil {
		entityJSON, err := json.Marshal(entity)
		if err != nil {
			entityString = fmt.Sprintf("[failed to marshal query entity: %v]", err)
			logging.Warnf("Failed to serialize entity for logging: %s", fmt.Sprint(entity))
		} else {
			entityString = string(entityJSON)
		}
	}
	bq.queryLogger.LogQuery(messageTemplate+entityString, err)
}

func (bq *BigQuery) Close() error {
	return bq.client.Close()
}

// Return true if google err is 404
func isNotFoundErr(err error) bool {
	e, ok := err.(*googleapi.Error)
	return ok && e.Code == http.StatusNotFound
}

// BQItem struct for streaming inserts to BigQuery
type BQItem struct {
	values map[string]interface{}
}

func (bqi *BQItem) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = map[string]bigquery.Value{}

	for k, v := range bqi.values {
		row[k] = v
	}

	return
}

func (bq *BigQuery) Update(ctx context.Context, tableName string, object types.Object, whenConditions *WhenConditions) (err error) {
	updateCondition, updateValues := bq.toWhenConditions(whenConditions)

	columns := make([]string, len(object), len(object))
	values := make([]bigquery.QueryParameter, len(object)+len(updateValues), len(object)+len(updateValues))
	i := 0
	for name, value := range object {
		columns[i] = name + "= @" + name
		values[i] = bigquery.QueryParameter{Name: name, Value: value}
		i++
	}
	for a := 0; a < len(updateValues); a++ {
		values[i+a] = updateValues[a]
	}
	updateQuery := fmt.Sprintf(updateBigQueryTemplate, bq.fullTableName(tableName), strings.Join(columns, ", "), updateCondition)
	defer func() {
		v := make([]any, len(values))
		for i, value := range values {
			v[i] = value.Value
		}
		if err != nil {
			err = errorj.UpdateError.Wrap(err, "failed execute update").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Dataset:   bq.config.Dataset,
					Table:     tableName,
					Statement: updateQuery,
					Values:    v,
				})
		}
	}()

	query := bq.client.Query(updateQuery)
	query.Parameters = values
	job, err := query.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	return status.Err()
}

func (bq *BigQuery) Select(ctx context.Context, tableName string, whenConditions *WhenConditions, orderBy string) ([]map[string]any, error) {
	return bq.selectFrom(ctx, tableName, "*", whenConditions, orderBy)
}
func (bq *BigQuery) selectFrom(ctx context.Context, tableName string, selectExpression string, whenConditions *WhenConditions, orderBy string) (res []map[string]any, err error) {
	whenCondition, values := bq.toWhenConditions(whenConditions)
	if whenCondition != "" {
		whenCondition = " WHERE " + whenCondition
	}
	if orderBy != "" {
		orderBy = " ORDER BY " + orderBy
	}
	selectQuery := fmt.Sprintf(selectBigQueryTemplate, selectExpression, bq.fullTableName(tableName), whenCondition, orderBy)
	bq.queryLogger.LogQuery(selectQuery, nil)

	defer func() {
		v := make([]any, len(values))
		for i, value := range values {
			v[i] = value.Value
		}
		if err != nil {
			err = errorj.SelectFromTableError.Wrap(err, "failed execute select").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Dataset:   bq.config.Dataset,
					Table:     tableName,
					Statement: selectQuery,
					Values:    v,
				})
		}
	}()

	query := bq.client.Query(selectQuery)
	query.Parameters = values
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := status.Err(); err != nil {
		return nil, err
	}
	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}
	var result []map[string]any
	for {
		var row = map[string]bigquery.Value{}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var resRow = map[string]any{}
		for k, v := range row {
			switch i := v.(type) {
			case int64:
				v = int(i)
			}
			resRow[k] = v
		}
		result = append(result, resRow)
	}
	return result, nil
}

func (bq *BigQuery) Count(ctx context.Context, tableName string, whenConditions *WhenConditions) (int, error) {
	res, err := bq.selectFrom(ctx, tableName, "count(*) as jitsu_count", whenConditions, "")
	if err != nil {
		return -1, err
	}
	if len(res) == 0 {
		return -1, fmt.Errorf("select count * gave no result")
	}
	return strconv.Atoi(fmt.Sprint(res[0]["jitsu_count"]))
}

func (bq *BigQuery) toWhenConditions(conditions *WhenConditions) (string, []bigquery.QueryParameter) {
	if conditions == nil {
		return "", []bigquery.QueryParameter{}
	}
	var queryConditions []string
	var values []bigquery.QueryParameter

	for _, condition := range conditions.Conditions {
		switch strings.ToLower(condition.Clause) {
		case "is null":
		case "is not null":
			queryConditions = append(queryConditions, condition.Field+" "+condition.Clause)
		default:
			queryConditions = append(queryConditions, condition.Field+" "+condition.Clause+" @when_"+condition.Field)
			values = append(values, bigquery.QueryParameter{Name: "when_" + condition.Field, Value: types.ReformatValue(condition.Value)})
		}
	}

	return strings.Join(queryConditions, " "+conditions.JoinCondition+" "), values
}
func (bq *BigQuery) Delete(ctx context.Context, tableName string, deleteConditions *WhenConditions) (err error) {
	whenCondition, values := bq.toWhenConditions(deleteConditions)
	if len(whenCondition) == 0 {
		return errors.New("delete conditions are empty")
	}
	deleteQuery := fmt.Sprintf(deleteBigQueryTemplate, bq.fullTableName(tableName), whenCondition)
	defer func() {
		v := make([]any, len(values))
		for i, value := range values {
			v[i] = value.Value
		}
		if err != nil {
			err = errorj.DeleteFromTableError.Wrap(err, "failed execute delete").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Dataset:   bq.config.Dataset,
					Table:     tableName,
					Statement: deleteQuery,
					Values:    v,
				})
		}
	}()
	query := bq.client.Query(deleteQuery)
	query.Parameters = values
	job, err := query.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	return status.Err()
}
func (bq *BigQuery) Type() string {
	return BigqueryBulkerTypeId
}

func (bq *BigQuery) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	return &TxSQLAdapter{sqlAdapter: bq, tx: NewDummyTxWrapper(bq.Type())}, nil
}

func (bq *BigQuery) fullTableName(tableName string) string {
	return bq.config.Project + "." + bq.config.Dataset + "." + tableName
}

//func (bq *BigQuery) createIndex(ctx context.Context, table *Table) (err error) {
//	createIndexQuery := fmt.Sprintf(createIndexBigQueryTemplate, table.PrimaryKeyName, bq.config.Project, bq.config.Dataset, table.Name, strings.Join(table.PKFields.ToSlice(), ","))
//	defer func() {
//		if err != nil {
//			err = errorj.CreatePrimaryKeysError.Wrap(err, "failed to create index").
//				WithProperty(errorj.DBInfo, &types.ErrorPayload{
//					Dataset:     bq.config.Dataset,
//					Table:       table.Name,
//					PrimaryKeys: table.PKFields.ToSlice(),
//					Statement:   createIndexQuery,
//				})
//		}
//	}()
//	query := bq.client.Query(createIndexQuery)
//	job, err := query.Run(ctx)
//	bq.logQuery(createIndexQuery, nil, err)
//	if err != nil {
//		return err
//	}
//	status, err := job.Wait(ctx)
//	if err != nil {
//		return err
//	}
//	return status.Err()
//}
//
//func (bq *BigQuery) dropIndex(ctx context.Context, table *Table) (err error) {
//	dropIndexQuery := fmt.Sprintf(dropIndexBigQueryTemplate, table.PrimaryKeyName, bq.config.Project, bq.config.Dataset, table.Name)
//	defer func() {
//		if err != nil {
//			err = errorj.DeletePrimaryKeysError.Wrap(err, "failed to drop index").
//				WithProperty(errorj.DBInfo, &types.ErrorPayload{
//					Dataset:     bq.config.Dataset,
//					Table:       table.Name,
//					PrimaryKeys: table.PKFields.ToSlice(),
//					Statement:   dropIndexQuery,
//				})
//		}
//	}()
//	query := bq.client.Query(dropIndexQuery)
//	job, err := query.Run(ctx)
//	bq.logQuery(dropIndexQuery, nil, err)
//	if err != nil {
//		return err
//	}
//	status, err := job.Wait(ctx)
//	if err != nil {
//		return err
//	}
//	return status.Err()
//}
