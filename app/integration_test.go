package app

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/jitsucom/bulker/app/testcontainers/kafka"
	"github.com/jitsucom/bulker/base/logging"
	_ "github.com/jitsucom/bulker/implementations/sql"
	"github.com/jitsucom/bulker/implementations/sql/testcontainers"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

const testBulkerPort = "33042"
const testBulkerURL = "http://localhost:" + testBulkerPort

type AppTestConfig struct {
	name                string
	destinationId       string
	eventsFile          string
	token               string
	expectedRowsCount   int
	expectedFailedCount int
	//map of expected errors by step name. May be error type or string. String is used for error message partial matching.
	expectedErrors map[string]any
}

var inited bool
var postgresContainer *testcontainers.PostgresContainer

func initContainers(t *testing.T) {
	if inited {
		return
	}
	_, err := kafka.NewKafkaContainer(context.Background())
	if err != nil {
		t.Fatalf("could not start kafka container: %v", err)
	}

	postgresContainer, err = testcontainers.NewPostgresContainer(context.Background(), "65432")
	if err != nil {
		t.Fatalf("could not start postgres container: %v", err)
	}

	go func() {
		_ = os.Setenv("HTTP_PORT", testBulkerPort)
		_ = os.Setenv("BULKER_CONFIG_PATH", "./test_data/")
		Run()
	}()
	ready := false
	//wait in loop for server readiness
	for i := 0; i < 240; i++ {
		res, err := http.Get(testBulkerURL + "/ready")
		if err == nil && res.StatusCode == 200 {
			ready = true
			logging.Infof("bulker is ready")
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("bulker is not ready")
	} else {
		inited = true
	}
}

// Test BulkerApp
func TestBulkerApp(t *testing.T) {
	initContainers(t)
	defer Exit()
	tests := []AppTestConfig{
		{
			name:                "good_batch",
			destinationId:       "batch_postgres",
			eventsFile:          "test_data/goodbatch.ndjson",
			token:               "21a2ae36-32994870a9fbf2f61ea6f6c8",
			expectedRowsCount:   30,
			expectedFailedCount: 0,
		},
		{
			name:                "bad_batch",
			destinationId:       "batch_postgres",
			eventsFile:          "test_data/badbatch.ndjson",
			token:               "21a2ae36-32994870a9fbf2f61ea6f6c8",
			expectedRowsCount:   20,
			expectedFailedCount: 10,
		},
		{
			name:                "good_stream",
			destinationId:       "stream_postgres",
			eventsFile:          "test_data/goodbatch.ndjson",
			token:               "21a2ae36-32994870a9fbf2f61ea6f6c8",
			expectedRowsCount:   30,
			expectedFailedCount: 0,
		},
		{
			name:                "bad_stream",
			destinationId:       "stream_postgres",
			eventsFile:          "test_data/badbatch.ndjson",
			token:               "21a2ae36-32994870a9fbf2f61ea6f6c8",
			expectedRowsCount:   29,
			expectedFailedCount: 1,
		},
		{
			name:          "invalid_token",
			destinationId: "stream_postgres",
			eventsFile:    "test_data/single.ndjson",
			token:         "badtoken",
			expectedErrors: map[string]any{
				"count_rows":         "pq: relation \"bulker.invalid_token\" does not exist",
				"send_object_0":      "unexpected status code: 401",
				"check_failed_topic": "unexpected status code: 401",
			},
			expectedRowsCount:   0,
			expectedFailedCount: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqr := require.New(t)

			file, err := os.Open(tt.eventsFile)
			if err != nil {
				t.Fatalf("could not open file %s: %v", tt.eventsFile, err)
			}
			scanner := bufio.NewScanner(file)
			i := 0
			for scanner.Scan() {
				req, _ := http.NewRequest("POST", fmt.Sprintf(testBulkerURL+"/post/%s?tableName=%s", tt.destinationId, tt.name), bytes.NewBuffer(scanner.Bytes()))
				req.Header.Set("Authorization", "Bearer "+tt.token)
				res, err := http.DefaultClient.Do(req)
				if err == nil {
					if res.StatusCode != 200 {
						err = fmt.Errorf("unexpected status code: %d", res.StatusCode)
					}
				}
				CheckError(fmt.Sprintf("send_object_%d", i), reqr, tt.expectedErrors, err)
				if res != nil && res.Body != nil {
					resBody, err := io.ReadAll(res.Body)
					_ = res.Body.Close()
					CheckError(fmt.Sprintf("read_response_%d", i), reqr, tt.expectedErrors, err)
					logging.Infof("response: %s", string(resBody))
				}
				i++
			}
			time.Sleep(15 * time.Second)
			rowsCount, err := postgresContainer.CountRows(fmt.Sprintf("%s.%s", postgresContainer.Schema, tt.name))
			CheckError("count_rows", reqr, tt.expectedErrors, err)
			logging.Infof("rows count: %d", rowsCount)

			reqr.Equal(tt.expectedRowsCount, rowsCount, "unexpected rows count in table %s", tt.name)

			req, _ := http.NewRequest("GET", fmt.Sprintf(testBulkerURL+"/failed/%s?tableName=%s", tt.destinationId, tt.name), nil)
			req.Header.Set("Authorization", "Bearer "+tt.token)
			res, err := http.DefaultClient.Do(req)
			if err == nil {
				if res.StatusCode != 200 {
					err = fmt.Errorf("unexpected status code: %d", res.StatusCode)
				}
			}
			CheckError("check_failed_topic", reqr, tt.expectedErrors, err)
			failedCount := 0
			if res.StatusCode == 200 {
				scanner = bufio.NewScanner(res.Body)
				for scanner.Scan() {
					logging.Infof("failed: %s", scanner.Text())
					failedCount++
				}
			}
			if res != nil && res.Body != nil {
				_ = res.Body.Close()
			}
			reqr.Equal(tt.expectedFailedCount, failedCount, "unexpected failed count for table %s", tt.name)
			logging.Infof("Test %s passed", tt.name)
		})
	}
}

func CheckError(step string, reqr *require.Assertions, expectedErrors map[string]any, err error) {
	expectedError := expectedErrors[step]
	switch target := expectedError.(type) {
	case string:
		reqr.Containsf(fmt.Sprintf("%v", err), target, "error in step %s doesn't contain expected value: %s", step, target)
	case error:
		reqr.ErrorIs(err, target, "error in step %s doesn't match expected error: %s", step, target)
	case nil:
		reqr.NoError(err, "unexpected error in step %s", step)
	default:
		panic(fmt.Sprintf("unexpected type of expected error: %T for step: %s", target, step))
	}
}
