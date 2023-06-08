package kafka

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/testcontainers/testcontainers-go"
	tcWait "github.com/testcontainers/testcontainers-go/wait"
)

type KafkaContainer struct {
	Compose *testcontainers.LocalDockerCompose
	Context context.Context
}

func NewKafkaContainer(ctx context.Context) (*KafkaContainer, error) {
	composeFilePaths := []string{"testcontainers/kafka/docker-compose.yml"}
	identifier := "bulker_kafka_compose"

	compose := testcontainers.NewLocalDockerCompose(composeFilePaths, identifier)
	execError := compose.Down()
	err := execError.Error
	if err != nil {
		logging.Errorf("couldnt down docker compose: %s : %v", compose.Identifier, err)
	}

	compose = testcontainers.NewLocalDockerCompose(composeFilePaths, identifier)
	execError = compose.
		WithCommand([]string{"up", "-d"}).
		WaitForService("kafka", tcWait.ForListeningPort("19092/tcp")).
		Invoke()
	err = execError.Error
	if err != nil {
		return nil, fmt.Errorf("could not run compose file: %v - %v", composeFilePaths, err)
	}

	return &KafkaContainer{
		Compose: compose,
		Context: ctx,
	}, nil
}

// Close terminates underlying docker container
func (ch *KafkaContainer) Close() error {
	if ch.Compose != nil {
		execError := ch.Compose.Down()
		err := execError.Error
		if err != nil {
			return fmt.Errorf("could down docker compose: %s", ch.Compose.Identifier)
		}
	}

	return nil
}
