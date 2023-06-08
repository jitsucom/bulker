package clickhouse

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/testcontainers/testcontainers-go"
	tcWait "github.com/testcontainers/testcontainers-go/wait"
	"time"
)

const (
	chClusterDatabase = "default"
	chClusterCluster  = "company_cluster"
)

var (
	chClusterHostsHttp   = []string{"localhost:8123", "localhost:8124", "localhost:8125", "localhost:8126"}
	chClusterHostsNative = []string{"localhost:9000", "localhost:9001", "localhost:9002", "localhost:9003"}
)

// ClickHouseClusterContainer is a ClickHouse testcontainer
type ClickHouseClusterContainer struct {
	Container testcontainers.Container
	Compose   *testcontainers.LocalDockerCompose
	Context   context.Context

	Cluster   string
	Hosts     []string
	HostsHTTP []string
	Database  string
}

// NewClickhouseClusterContainer creates new Clickhouse test container if CH_TEST_PORT is not defined. Otherwise uses db at defined port.
// This logic is required for running test at CI environment
func NewClickhouseClusterContainer(ctx context.Context) (*ClickHouseClusterContainer, error) {
	composeFilePaths := []string{"testcontainers/clickhouse/docker-compose.yml"}
	identifier := "bulker_clickhouse_cluster_compose"

	compose := testcontainers.NewLocalDockerCompose(composeFilePaths, identifier)
	execError := compose.Down()
	err := execError.Error
	if err != nil {
		logging.Errorf("couldnt down docker compose: %s : %v", compose.Identifier, err)
	}

	compose = testcontainers.NewLocalDockerCompose(composeFilePaths, identifier)
	execError = compose.
		WithCommand([]string{"up", "-d"}).
		WaitForService("clickhouse01", tcWait.ForListeningPort("9000/tcp").WithStartupTimeout(1*time.Minute)).
		WaitForService("clickhouse02", tcWait.ForListeningPort("9000/tcp").WithStartupTimeout(1*time.Minute)).
		WaitForService("clickhouse03", tcWait.ForListeningPort("9000/tcp").WithStartupTimeout(1*time.Minute)).
		WaitForService("clickhouse04", tcWait.ForListeningPort("9000/tcp").WithStartupTimeout(1*time.Minute)).
		Invoke()
	err = execError.Error
	if err != nil {
		return nil, fmt.Errorf("could not run compose file: %v - %v", composeFilePaths, err)
	}

	return &ClickHouseClusterContainer{
		Compose:   compose,
		Context:   ctx,
		Hosts:     chClusterHostsNative,
		HostsHTTP: chClusterHostsHttp,
		Database:  chClusterDatabase,
		Cluster:   chClusterCluster,
	}, nil
}

// Close terminates underlying docker container
func (ch *ClickHouseClusterContainer) Close() error {
	if ch.Compose != nil {
		execError := ch.Compose.Down()
		err := execError.Error
		if err != nil {
			return fmt.Errorf("could down docker compose: %s", ch.Compose.Identifier)
		}
	}

	return nil
}
