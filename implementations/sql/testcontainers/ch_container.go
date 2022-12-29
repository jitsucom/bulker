package testcontainers

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/testcontainers/testcontainers-go"
	tcWait "github.com/testcontainers/testcontainers-go/wait"
	"time"
)

const (
	chDatabase           = "default"
	chDatasourceTemplate = "clickhouse://default:@localhost:%d/default?read_timeout=5m&mutations_sync=2"

	envClickhousePortVariable = "CH_TEST_PORT"
)

// ClickHouseContainer is a ClickHouse testcontainer
type ClickHouseContainer struct {
	datasource *sql.DB
	Container  testcontainers.Container
	Context    context.Context

	Port     int
	Dsns     []string
	Database string
}

// NewClickhouseContainer creates new Clickhouse test container if CH_TEST_PORT is not defined. Otherwise uses db at defined port.
// This logic is required for running test at CI environment
func NewClickhouseContainer(ctx context.Context) (*ClickHouseContainer, error) {
	//if os.Getenv(envClickhousePortVariable) != "" {
	//	port, err := strconv.Atoi(os.Getenv(envClickhousePortVariable))
	//	if err != nil {
	//		return nil, err
	//	}
	//	dsn := fmt.Sprintf(chDatasourceTemplate, port)
	//
	//	datasource, err := sql.Open("clickhouse", dsn)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	return &ClickHouseContainer{
	//		datasource: datasource,
	//		Context:    ctx,
	//		Dsns:       []string{dsn},
	//		Database:   chDatabase,
	//		Port:       port,
	//	}, nil
	//}
	dbURL := func(port nat.Port) string {
		return fmt.Sprintf(chDatasourceTemplate, port.Int())
	}
	image := "clickhouse/clickhouse-server:22.8-alpine"
	exposedPortHttp := fmt.Sprintf("%d:%d", utils.GetPort(), 8123)
	exposedPortNative := fmt.Sprintf("%d:%d", utils.GetPort(), 9000)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{exposedPortHttp, exposedPortNative},
			WaitingFor:   tcWait.ForSQL("9000/tcp", "clickhouse", dbURL).Timeout(time.Second * 60),
			//WaitingFor:   tcWait.ForListeningPort("8123").WithStartupTimeout(1 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}

	port, err := container.MappedPort(ctx, "9000")
	if err != nil {
		container.Terminate(ctx)
		return nil, err
	}

	dsn := fmt.Sprintf(chDatasourceTemplate, port.Int())

	datasource, err := sql.Open("clickhouse", dsn)
	if err != nil {
		container.Terminate(ctx)
		return nil, err
	}

	return &ClickHouseContainer{
		datasource: datasource,
		Container:  container,
		Context:    ctx,
		Dsns:       []string{dsn},
		Database:   chDatabase,
		Port:       port.Int(),
	}, nil
}

//
////CountRows returns row count in DB table with name = table
////or error if occurred
//func (ch *ClickHouseContainer) CountRows(table string) (int, error) {
//	rows, err := ch.datasource.Query(fmt.Sprintf("SELECT count(*) from %s final", table))
//	if err != nil {
//		return -1, err
//	}
//	defer rows.Close()
//	rows.Next()
//	var count int
//	err = rows.Scan(&count)
//	return count, err
//}
//
//func (ch *ClickHouseContainer) GetSortedRows(table, selectClause, whereClause, orderClause string) ([]map[string]interface{}, error) {
//	where := ""
//	if whereClause != "" {
//		where = "where " + whereClause
//	}
//
//	rows, err := ch.datasource.Query(fmt.Sprintf("SELECT %s from %s %s %s", selectClause, table, where, orderClause))
//	if err != nil {
//		return nil, err
//	}
//	defer rows.Close()
//
//	return extractData(rows)
//}

// Close terminates underlying docker container
func (ch *ClickHouseContainer) Close() error {
	if ch.Container != nil {
		if err := ch.Container.Terminate(ch.Context); err != nil {
			logging.Errorf("Failed to stop ch container: %v", err)
		}
	}

	if ch.datasource != nil {
		if err := ch.datasource.Close(); err != nil {
			logging.Errorf("failed to close datasource in clickhouse container: %v", err)
		}
	}

	return nil
}

func (ch *ClickHouseContainer) Stop() error {
	return ch.Container.Stop(context.Background(), nil)
}

func (ch *ClickHouseContainer) Start() error {
	return ch.Container.Start(context.Background())
}
