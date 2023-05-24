package testcontainers

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/testcontainers/testcontainers-go"
	tcWait "github.com/testcontainers/testcontainers-go/wait"
)

const (
	minioAccessKey = "test_minio_access_key"
	minioSecretKey = "test_minio_secret_key"
)

// MinioContainer is a Min.IO testcontainer
type MinioContainer struct {
	Container testcontainers.Container
	Context   context.Context
	Host      string
	Port      int
	AccessKey string
	SecretKey string
}

// NewMinioContainer creates new MySQL test container if MYSQL_TEST_PORT is not defined. Otherwise uses db at defined port.
// This logic is required for running test at CI environment
func NewMinioContainer(ctx context.Context, bucketName string) (*MinioContainer, error) {
	dbSettings := make(map[string]string, 0)
	dbSettings["MINIO_ACCESS_KEY"] = minioAccessKey
	dbSettings["MINIO_SECRET_KEY"] = minioSecretKey

	exposedPort := fmt.Sprintf("%d:%d", utils.GetPort(), 9000)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "minio/minio:latest",
			Cmd:          []string{"server", "/data"},
			ExposedPorts: []string{exposedPort},
			Env:          dbSettings,
			WaitingFor:   tcWait.ForListeningPort("9000"),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, err
	}

	port, err := container.MappedPort(ctx, "9000")
	if err != nil {
		container.Terminate(ctx)
		return nil, err
	}
	mc := MinioContainer{
		Container: container,
		Context:   ctx,
		Host:      host,
		Port:      port.Int(),
		AccessKey: minioAccessKey,
		SecretKey: minioSecretKey,
	}
	err = mc.createBucket(bucketName)
	if err != nil {
		_ = mc.Close()
		return nil, err
	}
	return &mc, nil
}

func (mc *MinioContainer) createBucket(bucketName string) error {
	awsConfig := aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(minioAccessKey, minioSecretKey, "")).
		WithRegion("us-east-1").WithEndpoint(fmt.Sprintf("http://%s:%d", mc.Host, mc.Port)).WithS3ForcePathStyle(true)

	s3Session, err := session.NewSession()
	if err != nil {
		return err
	}
	client := s3.New(s3Session, awsConfig)
	_, err = client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return err
	}
	return nil

}

// Close terminates underlying mysql docker container
func (mc *MinioContainer) Close() error {
	if mc.Container != nil {
		err := mc.Container.Terminate(mc.Context)
		if err != nil {
			logging.Errorf("Failed to stop MySQL container: %v", err)
		}
	}

	return nil
}

func (mc *MinioContainer) Stop() error {
	return mc.Container.Stop(context.Background(), nil)
}

func (mc *MinioContainer) Start() error {
	return mc.Container.Start(context.Background())
}
