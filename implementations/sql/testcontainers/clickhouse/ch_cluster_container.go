package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/testcontainers/testcontainers-go"
	tcWait "github.com/testcontainers/testcontainers-go/wait"
)

const (
	chClusterDatabase = "default"
	chClusterCluster  = "company_cluster"
	//chClusterDatasourceTemplate = "http://default:@localhost:8123,localhost:8124,localhost:8125,localhost:8126/default?read_timeout=5m&mutations_sync=2&connection_open_strategy=round_robin"
	chClusterDatasourceTemplate = "clickhouse://default:@localhost:9000,localhost:9001,localhost:9002,localhost:9003/default?read_timeout=5m&mutations_sync=2&connection_open_strategy=round_robin"
)

// ClickHouseClusterContainer is a ClickHouse testcontainer
type ClickHouseClusterContainer struct {
	datasource *sql.DB
	Container  testcontainers.Container
	Compose    *testcontainers.LocalDockerCompose
	Context    context.Context

	Cluster  string
	Dsns     []string
	Database string
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
		WaitForService("clickhouse01", tcWait.ForListeningPort("9000/tcp")).
		WaitForService("clickhouse02", tcWait.ForListeningPort("9000/tcp")).
		WaitForService("clickhouse03", tcWait.ForListeningPort("9000/tcp")).
		WaitForService("clickhouse04", tcWait.ForListeningPort("9000/tcp")).
		Invoke()
	err = execError.Error
	if err != nil {
		return nil, fmt.Errorf("could not run compose file: %v - %v", composeFilePaths, err)
	}

	datasource, err := sql.Open("clickhouse", chClusterDatasourceTemplate)
	if err != nil {
		execError := compose.Down()
		err := execError.Error
		if err != nil {
			return nil, fmt.Errorf("could not run compose file: %v - %v", composeFilePaths, err)
		}
		return nil, err
	}

	return &ClickHouseClusterContainer{
		datasource: datasource,
		Compose:    compose,
		Context:    ctx,
		Dsns:       []string{chClusterDatasourceTemplate},
		Database:   chClusterDatabase,
		Cluster:    chClusterCluster,
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

	if ch.datasource != nil {
		if err := ch.datasource.Close(); err != nil {
			logging.Errorf("failed to close datasource in clickhouse container: %v", err)
		}
	}

	return nil
}
