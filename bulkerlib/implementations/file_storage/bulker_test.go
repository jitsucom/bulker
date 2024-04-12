package file_storage

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/implementations"
	"github.com/jitsucom/bulker/bulkerlib/implementations/file_storage/testcontainers"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

var constantTimeStr = "2022-08-18T14:17:22.375Z"
var constantTime = timestamp.MustParseTime(time.RFC3339Nano, constantTimeStr)

var allBulkerConfigs []string

type TestConfig struct {
	//type of bulker destination
	BulkerType string
	//Config     destination config
	Config any
}

type StepFunction func(testConfig bulkerTestConfig, mode bulker.BulkMode) error

var configRegistry = map[string]any{}

var minioContainer *testcontainers.MinioContainer

func init() {
	gcsConfig := os.Getenv("BULKER_TEST_GCS")
	if gcsConfig != "" {
		configRegistry[GCSBulkerTypeId] = TestConfig{BulkerType: GCSBulkerTypeId, Config: gcsConfig}
	}
	s3Config := os.Getenv("BULKER_TEST_S3")
	if s3Config != "" {
		configRegistry[S3BulkerTypeId] = TestConfig{BulkerType: S3BulkerTypeId, Config: s3Config}
	} else {
		var err error
		minioContainer, err = testcontainers.NewMinioContainer(context.Background(), "bulkertests")
		if err != nil {
			panic(err)
		}
		configRegistry[S3BulkerTypeId+"_gzip"] = TestConfig{BulkerType: S3BulkerTypeId, Config: implementations.S3Config{
			FileConfig: implementations.FileConfig{
				Folder:      "tests",
				Format:      types.FileFormatNDJSON,
				Compression: types.FileCompressionGZIP,
			},
			Endpoint:  fmt.Sprintf("http://%s:%d", minioContainer.Host, minioContainer.Port),
			Region:    "us-east-1",
			Bucket:    "bulkertests",
			AccessKey: minioContainer.AccessKey,
			SecretKey: minioContainer.SecretKey,
		}}
		configRegistry[S3BulkerTypeId] = TestConfig{BulkerType: S3BulkerTypeId, Config: implementations.S3Config{
			FileConfig: implementations.FileConfig{
				Folder:      "tests",
				Format:      types.FileFormatNDJSON,
				Compression: types.FileCompressionNONE,
			},
			Endpoint:  fmt.Sprintf("http://%s:%d", minioContainer.Host, minioContainer.Port),
			Region:    "us-east-1",
			Bucket:    "bulkertests",
			AccessKey: minioContainer.AccessKey,
			SecretKey: minioContainer.SecretKey,
		}}
	}

	allBulkerConfigs = make([]string, 0, len(configRegistry))
	for k := range configRegistry {
		allBulkerConfigs = append(allBulkerConfigs, k)
	}
	////uncomment to run test for single db only
	//allBulkerConfigs = []string{S3BulkerTypeId}
	//exceptBigquery = allBulkerConfigs
	logging.Infof("Initialized bulker types: %v", allBulkerConfigs)
}

type bulkerTestConfig struct {
	//name of the test
	name string
	//tableName name of the destination table. Leave empty generate automatically
	tableName string
	//bulker config
	config *bulker.Config
	//for which bulker predefined configurations to run test
	configIds []string
	//continue test run even after Consume() returned error
	ignoreConsumeErrors bool
	//expected state of stream Complete() call
	expectedState *bulker.State
	//rows count expected in resulting table. don't use with expectedRows. any type to allow nil value meaning not set
	expectedRowsCount any
	//rows data expected in resulting table
	expectedRows []map[string]any
	//for configs that runs for multiple modes including bulker.ReplacePartition automatically adds WithPartition to streamOptions and takes into account partitionId in expected file name
	expectPartitionId bool
	//map of expected errors by step name. May be error type or string. String is used for error message partial matching.
	expectedErrors map[string]any
	//map of function to run after each step by step name.
	postStepFunctions map[string]StepFunction
	//don't clean up resulting file before and after test run.
	leaveResultingFile bool
	//file with objects to consume in ngjson format
	dataFile string
	//bulker stream mode-s to test
	modes []bulker.BulkMode
	//bulker stream options
	streamOptions []bulker.StreamOption
	//batchSize for bigdata test commit stream every batchSize rows
	batchSize int
}

func (c *bulkerTestConfig) adaptConfig(mode bulker.BulkMode, fileAdapter implementations.FileAdapter) (id, tableName, expectedFileName string) {
	tableName = c.tableName
	if tableName == "" {
		tableName = c.name
	}
	tableName = tableName + "_" + strings.ToLower(string(mode))
	id = fmt.Sprintf("%s_%s", c.config.BulkerType, tableName)
	expectedFileName = tableName
	ext := ""
	switch fileAdapter.Format() {
	case types.FileFormatNDJSON, types.FileFormatNDJSONFLAT:
		ext = ".ndjson"
	case types.FileFormatCSV:
		ext = ".csv"
	}
	if fileAdapter.Compression() == types.FileCompressionGZIP {
		ext = ext + ".gz"
	}
	switch mode {
	case bulker.ReplacePartition:
		if c.expectPartitionId {
			partitionId := uuid.New()
			newOptions := make([]bulker.StreamOption, len(c.streamOptions))
			copy(newOptions, c.streamOptions)
			newOptions = append(newOptions, bulker.WithPartition(partitionId))
			c.streamOptions = newOptions
			expectedFileName = fmt.Sprintf("%s/%s", expectedFileName, partitionId)
		}
	case bulker.Batch:
		expectedFileName = fmt.Sprintf("%s_%s", expectedFileName, constantTime.Format(FilenameDate))
	}
	expectedFileName = expectedFileName + ext
	return
}

func TestBasics(t *testing.T) {
	timestamp.SetFreezeTime(constantTime)
	timestamp.FreezeTime()
	defer timestamp.UnfreezeTime()
	tests := []bulkerTestConfig{
		{
			name:               "repeated_ids_no_pk",
			modes:              []bulker.BulkMode{bulker.Batch, bulker.ReplaceTable, bulker.ReplacePartition},
			dataFile:           "test_data/repeated_ids.ndjson",
			expectPartitionId:  true,
			leaveResultingFile: true,
			expectedRows: []map[string]any{
				{"_timestamp": constantTimeStr, "id": 1, "name": "test"},
				{"_timestamp": constantTimeStr, "id": 2, "name": "test1"},
				{"_timestamp": constantTimeStr, "id": 3, "name": "test2"},
				{"_timestamp": constantTimeStr, "id": 3, "name": "test3"},
				{"_timestamp": constantTimeStr, "id": 4, "name": "test4"},
				{"_timestamp": constantTimeStr, "id": 4, "name": "test5"},
				{"_timestamp": constantTimeStr, "id": 3, "name": "test6"},
				{"_timestamp": constantTimeStr, "id": 1, "name": "test7"},
			},
			configIds: allBulkerConfigs,
		},
		{
			name:              "repeated_ids_pk",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.ReplaceTable, bulker.ReplacePartition},
			dataFile:          "test_data/repeated_ids.ndjson",
			expectPartitionId: true,
			expectedRows: []map[string]any{
				{"_timestamp": constantTimeStr, "id": 2, "name": "test1"},
				{"_timestamp": constantTimeStr, "id": 4, "name": "test5"},
				{"_timestamp": constantTimeStr, "id": 3, "name": "test6"},
				{"_timestamp": constantTimeStr, "id": 1, "name": "test7"},
			},
			configIds:     allBulkerConfigs,
			streamOptions: []bulker.StreamOption{bulker.WithPrimaryKey("id"), bulker.WithDeduplicate()},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			//t.Parallel()
			runTestConfig(t, tt, testStream)
		})
	}
}

func runTestConfig(t *testing.T, tt bulkerTestConfig, testFunc func(*testing.T, bulkerTestConfig, bulker.BulkMode)) {
	if tt.config != nil {
		for _, mode := range tt.modes {
			t.Run(string(mode)+"_"+tt.config.Id+"_"+tt.name, func(t *testing.T) {
				testFunc(t, tt, mode)
			})
		}
	} else {
		for _, testConfigId := range tt.configIds {
			newTd := tt
			if !utils.ArrayContains(allBulkerConfigs, testConfigId) {
				t.Skipf("Config '%s' is not selected for this test", testConfigId)
			}
			testConfigRaw, ok := configRegistry[testConfigId]
			if !ok {
				t.Fatalf("No config found for %s", testConfigId)
			}
			testConfig := testConfigRaw.(TestConfig)
			newTd.config = &bulker.Config{Id: testConfigId, BulkerType: testConfig.BulkerType, DestinationConfig: testConfig.Config, LogLevel: bulker.Verbose}
			for _, mode := range newTd.modes {
				tc := newTd
				mode := mode
				t.Run(string(mode)+"_"+testConfigId+"_"+newTd.name, func(t *testing.T) {
					testFunc(t, tc, mode)
				})
			}
		}
	}
}

func testStream(t *testing.T, testConfig bulkerTestConfig, mode bulker.BulkMode) {
	reqr := require.New(t)
	PostStep("init", testConfig, mode, reqr, nil)
	blk, err := bulker.CreateBulker(*testConfig.config)
	PostStep("create_bulker", testConfig, mode, reqr, err)
	defer func() {
		err = blk.Close()
		PostStep("bulker_close", testConfig, mode, reqr, err)
	}()
	fileAdapter, ok := blk.(implementations.FileAdapter)
	reqr.True(ok)
	ctx := context.Background()
	id, tableName, expectedFileName := testConfig.adaptConfig(mode, fileAdapter)

	//clean up in case of previous test failure
	if !testConfig.leaveResultingFile {
		_ = fileAdapter.DeleteObject(expectedFileName)
	}
	//clean up after test run
	if !testConfig.leaveResultingFile {
		defer func() {
			err = fileAdapter.DeleteObject(expectedFileName)
		}()
	}
	stream, err := blk.CreateStream(id, tableName, mode, testConfig.streamOptions...)
	PostStep("create_stream", testConfig, mode, reqr, err)
	if err != nil {
		return
	}
	//Abort stream if error occurred
	defer func() {
		if err != nil {
			_, _ = stream.Abort(ctx)
			//CheckError("stream_abort", testConfig.config.BulkerType, reqr, testConfig.expectedErrors, err)
		}
	}()

	file, err := os.Open(testConfig.dataFile)
	PostStep("open_file", testConfig, mode, reqr, err)
	defer func() {
		_ = file.Close()
	}()
	scanner := bufio.NewScanner(file)
	i := 0
	streamNum := 0
	for scanner.Scan() {
		if i > 0 && testConfig.batchSize > 0 && i%testConfig.batchSize == 0 {
			_, err := stream.Complete(ctx)
			PostStep(fmt.Sprintf("stream_complete_%d", streamNum), testConfig, mode, reqr, err)
			streamNum++
			logging.Infof("%d. batch is completed", i)
			stream, err = blk.CreateStream(id, tableName, mode, testConfig.streamOptions...)
			PostStep(fmt.Sprintf("create_stream_%d", streamNum), testConfig, mode, reqr, err)
			if err != nil {
				return
			}
		}
		obj := types.Object{}
		decoder := jsoniter.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()
		err = decoder.Decode(&obj)
		PostStep("decode_json", testConfig, mode, reqr, err)
		_, _, err = stream.Consume(ctx, obj)
		PostStep(fmt.Sprintf("consume_object_%d", i), testConfig, mode, reqr, err)
		if err != nil && !testConfig.ignoreConsumeErrors {
			break
		}
		i++
	}
	//Commit stream
	state, err := stream.Complete(ctx)
	PostStep("stream_complete", testConfig, mode, reqr, err)

	if testConfig.expectedState != nil {
		reqr.Equal(*testConfig.expectedState, state)
	}
	if err != nil {
		return
	}
	//PostStep("state_lasterror", testConfig, mode, reqr, state.LastError)
	if testConfig.expectedRowsCount != nil || testConfig.expectedRows != nil {
		time.Sleep(1 * time.Second)
		//Check rows count and rows data when provided
		rowBytes, err := fileAdapter.Download(expectedFileName)
		rows := []map[string]any{}
		var reader io.Reader
		if fileAdapter.Compression() == types.FileCompressionGZIP {
			reader, _ = gzip.NewReader(bytes.NewReader(rowBytes))
		} else {
			reader = bytes.NewReader(rowBytes)
		}
		//read rows from rowBytes using Scanner
		scanner = bufio.NewScanner(reader)
		for scanner.Scan() {
			scannerBytes := scanner.Bytes()
			row := map[string]any{}
			decoder := jsoniter.NewDecoder(bytes.NewReader(scannerBytes))
			decoder.UseNumber()
			err = decoder.Decode(&row)
			PostStep("decode_result_json", testConfig, mode, reqr, err)
			for k, v := range row {
				num, ok := v.(json.Number)
				if ok {
					i, err := num.Int64()
					if err != nil {
						row[k], err = num.Float64()
					} else {
						row[k] = int(i)
					}
					if err != nil {
						row[k] = v
					}
				}
			}
			rows = append(rows, row)
		}
		PostStep("select_result", testConfig, mode, reqr, err)
		if testConfig.expectedRows == nil {
			reqr.Equal(testConfig.expectedRowsCount, len(rows))
		} else {
			reqr.Equal(testConfig.expectedRows, rows)
		}
	}
}

func PostStep(step string, testConfig bulkerTestConfig, mode bulker.BulkMode, reqr *require.Assertions, err error) {
	bulkerType := testConfig.config.BulkerType
	stepLookupKeys := []string{step + "_" + bulkerType + "_" + strings.ToLower(string(mode)), step + "_" + bulkerType, step}

	//run post step function if any
	stepF := utils.MapNVLKeys(testConfig.postStepFunctions, stepLookupKeys...)
	if stepF != nil {
		err1 := stepF(testConfig, mode)
		if err == nil {
			err = err1
		}
	}

	expectedError := utils.MapNVLKeys(testConfig.expectedErrors, stepLookupKeys...)
	switch target := expectedError.(type) {
	case []string:
		contains := false
		for _, t := range target {
			if strings.Contains(err.Error(), t) {
				contains = true
				break
			}
		}
		if !contains {
			reqr.Fail(fmt.Sprintf("%s", err), "error in step %s doesn't contain one of expected value: %+v", step, target)
		}
	case string:
		reqr.ErrorContainsf(err, target, "error in step %s doesn't contain expected value: %s", step, target)
	case error:
		reqr.ErrorIs(err, target, "error in step %s doesn't match expected error: %s", step, target)
	case nil:
		reqr.NoError(err, "unexpected error in step %s", step)
	default:
		panic(fmt.Sprintf("unexpected type of expected error: %T for step: %s", target, step))
	}
}
