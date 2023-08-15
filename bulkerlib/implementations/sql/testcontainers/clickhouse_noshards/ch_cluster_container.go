package clickhouse_noshards

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/testcontainers/testcontainers-go"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

const (
	chClusterDatabase = "default"
	chClusterCluster  = "company_cluster"
)

var (
	chClusterHostsHttp   = []string{"localhost:8133", "localhost:8134"}
	chClusterHostsNative = []string{"localhost:9010", "localhost:9011"}
)

// ClickHouseClusterContainerNoShards is a ClickHouse testcontainer
type ClickHouseClusterContainerNoShards struct {
	Identifier string
	Container  testcontainers.Container
	Compose    tc.ComposeStack
	Context    context.Context

	Cluster   string
	Hosts     []string
	HostsHTTP []string
	Database  string
}

// ClickHouseClusterContainerNoShards creates new Clickhouse test container if CH_TEST_PORT is not defined. Otherwise uses db at defined port.
// This logic is required for running test at CI environment
func NewClickHouseClusterContainerNoShards(ctx context.Context) (*ClickHouseClusterContainerNoShards, error) {
	composeFilePaths := "testcontainers/clickhouse_noshards/docker-compose.yml"
	identifier := "bulker_clickhouse_cluster_noshards_compose"

	compose, err := tc.NewDockerComposeWith(tc.WithStackFiles(composeFilePaths), tc.StackIdentifier(identifier))
	if err != nil {
		logging.Errorf("couldnt down docker compose: %s : %v", identifier, err)
	}
	err = compose.Down(ctx)
	if err != nil {
		logging.Errorf("couldnt down docker compose: %s : %v", identifier, err)
	}

	compose, err = tc.NewDockerComposeWith(tc.WithStackFiles(composeFilePaths), tc.StackIdentifier(identifier))
	if err != nil {
		return nil, fmt.Errorf("could not run compose file: %v - %v", composeFilePaths, err)
	}
	err = compose.Up(ctx, tc.Wait(true))
	if err != nil {
		return nil, fmt.Errorf("could not run compose file: %v - %v", composeFilePaths, err)
	}

	return &ClickHouseClusterContainerNoShards{
		Identifier: identifier,
		Compose:    compose,
		Context:    ctx,
		Hosts:      chClusterHostsNative,
		HostsHTTP:  chClusterHostsHttp,
		Database:   chClusterDatabase,
		Cluster:    chClusterCluster,
	}, nil
}

// Close terminates underlying docker container
func (ch *ClickHouseClusterContainerNoShards) Close() error {
	if ch.Compose != nil {
		err := ch.Compose.Down(context.Background())
		if err != nil {
			return fmt.Errorf("could down docker compose: %s", ch.Identifier)
		}
	}

	return nil
}
