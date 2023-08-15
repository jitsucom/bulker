package kafka

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/logging"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

type KafkaContainer struct {
	Identifier string
	Compose    tc.ComposeStack
	Context    context.Context
}

func NewKafkaContainer(ctx context.Context) (*KafkaContainer, error) {
	composeFilePaths := "testcontainers/kafka/docker-compose.yml"
	identifier := "bulker_kafka_compose"

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

	return &KafkaContainer{
		Identifier: identifier,
		Compose:    compose,
		Context:    ctx,
	}, nil
}

// Close terminates underlying docker container
func (ch *KafkaContainer) Close() error {
	if ch.Compose != nil {
		err := ch.Compose.Down(context.Background())
		if err != nil {
			return fmt.Errorf("could down docker compose: %s", ch.Identifier)
		}
	}

	return nil
}
