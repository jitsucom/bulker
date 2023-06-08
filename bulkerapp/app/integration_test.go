package app

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"github.com/jitsucom/bulker/bulkerapp/testcontainers/kafka"
	_ "github.com/jitsucom/bulker/bulkerlib/implementations/sql"
	"github.com/jitsucom/bulker/bulkerlib/implementations/sql/testcontainers"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"
	"time"
)

const testBulkerPort = "33042"
const testBulkerURL = "http://localhost:" + testBulkerPort

type StepFunction func() error

type AppTestConfig struct {
	name                       string
	destinationId              string
	eventsFile                 string
	token                      string
	expectedRowsCount          int
	expectedDeadCount          int
	waitAfterAllMessageSentSec int
	//map of expected errors by step name. May be error type or string. String is used for error message partial matching.
	expectedErrors map[string]any
	//map of function to run after each step by step name.
	postStepFunctions map[string]StepFunction
}

//go:embed test_data/config.yaml
var testConfigSource string

//go:embed test_data/bulker.env
var testBulkerEnv string

func initApp(t *testing.T, envVars map[string]string) (app *AppContext, kafkaContainer *kafka.KafkaContainer, postgresContainer *testcontainers.PostgresContainer) {
	var err error
	postgresContainer, err = testcontainers.NewPostgresContainer(context.Background(), true)
	if err != nil {
		t.Fatalf("could not start postgres container: %v", err)
		return
	}
	kafkaContainer, err = kafka.NewKafkaContainer(context.Background())
	if err != nil {
		t.Fatalf("could not start kafka container: %v", err)
		return
	}
	dir := t.TempDir()
	cfg := strings.ReplaceAll(testConfigSource, "[[POSTGRES_PORT]]", fmt.Sprint(postgresContainer.Port))
	err = os.WriteFile(path.Join(dir, "config.yaml"), []byte(cfg), 0644)
	if err != nil {
		t.Fatalf("could not write config.yaml to temp file: %v", err)
	}
	env := strings.ReplaceAll(testBulkerEnv, "[[CONFIG_SOURCE]]", path.Join(dir, "config.yaml"))
	err = os.WriteFile(path.Join(dir, "bulker.env"), []byte(env), 0644)
	if err != nil {
		t.Fatalf("could not write bulker.env to temp file: %v", err)
	}
	t.Setenv("HTTP_PORT", testBulkerPort)
	t.Setenv("BULKER_CONFIG_PATH", dir)
	for k, v := range envVars {
		t.Setenv(k, v)
	}
	app = InitAppContext()
	go func() {
		app.server.ListenAndServe()
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
		time.Sleep(1000 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("bulker is not ready")
	}
	return
}

// Test streams in autocommit and bath mode. Both with good batches and batches with primary key violation error
func TestGoodAndBadStreams(t *testing.T) {
	app, kafka, postgresContainer := initApp(t, map[string]string{"BULKER_MESSAGES_RETRY_COUNT": "0",
		"BULKER_BATCH_RUNNER_DEFAULT_PERIOD_SEC": "1"})
	t.Cleanup(func() {
		app.Shutdown()
		if postgresContainer != nil {
			_ = postgresContainer.Close()
		}
		if kafka != nil {
			_ = kafka.Close()
		}
	})

	tests := []AppTestConfig{
		{
			name:                       "good_batch",
			destinationId:              "batch_postgres",
			eventsFile:                 "test_data/goodbatch.ndjson",
			token:                      "21a2ae36-32994870a9fbf2f61ea6f6c8",
			waitAfterAllMessageSentSec: 10,
			expectedRowsCount:          30,
			expectedDeadCount:          0,
		},
		{
			name:                       "bad_batch",
			destinationId:              "batch_postgres",
			eventsFile:                 "test_data/badbatch.ndjson",
			token:                      "21a2ae36-32994870a9fbf2f61ea6f6c8",
			waitAfterAllMessageSentSec: 10,
			expectedRowsCount:          20,
			expectedDeadCount:          10,
		},
		{
			name:                       "good_stream",
			destinationId:              "stream_postgres",
			eventsFile:                 "test_data/goodbatch.ndjson",
			token:                      "21a2ae36-32994870a9fbf2f61ea6f6c8",
			waitAfterAllMessageSentSec: 10,
			expectedRowsCount:          30,
			expectedDeadCount:          0,
		},
		{
			name:                       "bad_stream",
			destinationId:              "stream_postgres",
			eventsFile:                 "test_data/badbatch.ndjson",
			token:                      "21a2ae36-32994870a9fbf2f61ea6f6c8",
			waitAfterAllMessageSentSec: 10,
			expectedRowsCount:          29,
			expectedDeadCount:          1,
		},
		{
			name:                       "invalid_token",
			destinationId:              "stream_postgres",
			eventsFile:                 "test_data/single.ndjson",
			waitAfterAllMessageSentSec: 0,
			token:                      "badtoken",
			expectedErrors: map[string]any{
				"count_rows":       "pq: relation \"bulker.invalid_token\" does not exist",
				"send_object_0":    "unexpected status code: 401",
				"check_dead_topic": "unexpected status code: 401",
			},
			expectedRowsCount: 0,
			expectedDeadCount: 0,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			integrationTest(t, tt, postgresContainer)
		})
	}
}

// Test that retry consumer works
func TestEventsRetry(t *testing.T) {
	app, kafka, postgresContainer := initApp(t, map[string]string{"BULKER_MESSAGES_RETRY_COUNT": "20",
		"BULKER_BATCH_RUNNER_DEFAULT_RETRY_PERIOD_SEC":     "5",
		"BULKER_BATCH_RUNNER_DEFAULT_RETRY_BATCH_FRACTION": "1",
		"BULKER_MESSAGES_RETRY_BACKOFF_BASE":               "0",
		"BULKER_BATCH_RUNNER_DEFAULT_PERIOD_SEC":           "1"})
	t.Cleanup(func() {
		app.Shutdown()
		if postgresContainer != nil {
			_ = postgresContainer.Close()
		}
		if kafka != nil {
			_ = kafka.Close()
		}
	})
	tests := []AppTestConfig{
		{
			name:                       "good_batch",
			destinationId:              "batch_postgres",
			eventsFile:                 "test_data/goodbatch.ndjson",
			token:                      "21a2ae36-32994870a9fbf2f61ea6f6c8",
			waitAfterAllMessageSentSec: 10,
			postStepFunctions: map[string]StepFunction{
				"send_object_9": func() error {
					time.Sleep(5 * time.Second)
					return postgresContainer.Stop()
				},
				"send_object_19": func() error {
					time.Sleep(10 * time.Second)
					return postgresContainer.Start()
				},
			},
			expectedRowsCount: 30,
			expectedDeadCount: 0,
		},
		{
			name:                       "good_stream",
			destinationId:              "stream_postgres",
			eventsFile:                 "test_data/goodbatch.ndjson",
			token:                      "21a2ae36-32994870a9fbf2f61ea6f6c8",
			waitAfterAllMessageSentSec: 10,
			postStepFunctions: map[string]StepFunction{
				"send_object_9": func() error {
					time.Sleep(5 * time.Second)
					return postgresContainer.Stop()
				},
				"send_object_19": func() error {
					time.Sleep(10 * time.Second)
					return postgresContainer.Start()
				},
			},
			expectedRowsCount: 30,
			expectedDeadCount: 0,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			integrationTest(t, tt, postgresContainer)
		})
	}
}

func integrationTest(t *testing.T, tt AppTestConfig, postgresContainer *testcontainers.PostgresContainer) {
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
		PostStep(fmt.Sprintf("send_object_%d", i), tt, reqr, err)
		if res != nil && res.Body != nil {
			resBody, err := io.ReadAll(res.Body)
			_ = res.Body.Close()
			PostStep(fmt.Sprintf("read_response_%d", i), tt, reqr, err)
			logging.Infof("response: %s", string(resBody))
		}
		i++
	}
	time.Sleep(time.Duration(tt.waitAfterAllMessageSentSec) * time.Second)
	rowsCount, err := postgresContainer.CountRows(fmt.Sprintf("%s.%s", postgresContainer.Schema, tt.name))
	PostStep("count_rows", tt, reqr, err)
	logging.Infof("rows count: %d", rowsCount)

	reqr.Equal(tt.expectedRowsCount, rowsCount, "unexpected rows count in table %s", tt.name)

	req, _ := http.NewRequest("GET", fmt.Sprintf(testBulkerURL+"/failed/%s?tableName=%s&status=dead", tt.destinationId, tt.name), nil)
	req.Header.Set("Authorization", "Bearer "+tt.token)
	res, err := http.DefaultClient.Do(req)
	if err == nil {
		if res.StatusCode != 200 {
			err = fmt.Errorf("unexpected status code: %d", res.StatusCode)
		}
	}
	PostStep("check_dead_topic", tt, reqr, err)
	deadCount := 0
	if res.StatusCode == 200 {
		scanner = bufio.NewScanner(res.Body)
		for scanner.Scan() {
			logging.Infof("dead: %s", scanner.Text())
			deadCount++
		}
	}
	if res != nil && res.Body != nil {
		_ = res.Body.Close()
	}
	reqr.Equal(tt.expectedDeadCount, deadCount, "unexpected dead count for table %s", tt.name)
	logging.Infof("Test %s passed", tt.name)
}

func PostStep(step string, testConfig AppTestConfig, reqr *require.Assertions, err error) {
	//run post step function if any
	stepF := testConfig.postStepFunctions[step]
	if stepF != nil {
		err1 := stepF()
		if err == nil {
			err = err1
		}
	}

	expectedError := testConfig.expectedErrors[step]
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
