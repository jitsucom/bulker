package implementations

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"go.uber.org/atomic"
	"io"
	"time"
)

// S3Config is a dto for config deserialization
type S3Config struct {
	FileConfig `mapstructure:",squash" json:",inline" yaml:",inline"`
	AccessKey  string `mapstructure:"accessKeyId,omitempty" json:"accessKeyId,omitempty" yaml:"accessKeyId,omitempty"`
	SecretKey  string `mapstructure:"secretAccessKey,omitempty" json:"secretAccessKey,omitempty" yaml:"secretAccessKey,omitempty"`
	Bucket     string `mapstructure:"bucket,omitempty" json:"bucket,omitempty" yaml:"bucket,omitempty"`
	Region     string `mapstructure:"region,omitempty" json:"region,omitempty" yaml:"region,omitempty"`
	Endpoint   string `mapstructure:"endpoint,omitempty" json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
}

// Validate returns err if invalid
func (s3c *S3Config) Validate() error {
	if s3c == nil {
		return errors.New("S3 config is required")
	}
	if s3c.AccessKey == "" {
		return errors.New("S3 accessKey is required parameter")
	}
	if s3c.SecretKey == "" {
		return errors.New("S3 secretKey is required parameter")
	}
	if s3c.Bucket == "" {
		return errors.New("S3 bucket is required parameter")
	}
	if s3c.Region == "" {
		return errors.New("S3 region is required parameter")
	}
	return nil
}

// S3 is a S3 adapter for uploading/deleting files
type S3 struct {
	AbstractFileAdapter
	config *S3Config
	client *s3.S3

	closed *atomic.Bool
}

// NewS3 returns configured S3 adapter
func NewS3(s3Config *S3Config) (*S3, error) {
	if err := s3Config.Validate(); err != nil {
		return nil, err
	}

	awsConfig := aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(s3Config.AccessKey, s3Config.SecretKey, "")).
		WithRegion(s3Config.Region)
	if s3Config.Endpoint != "" {
		awsConfig.WithEndpoint(s3Config.Endpoint)
		awsConfig.WithS3ForcePathStyle(true)
	}
	if s3Config.Format == "" {
		s3Config.Format = types2.FileFormatNDJSON
	}
	s3Session, err := session.NewSession()
	if err != nil {
		return nil, errorj.SaveOnStageError.Wrap(err, "failed to create s3 session")
	}

	return &S3{AbstractFileAdapter: AbstractFileAdapter{config: &s3Config.FileConfig}, client: s3.New(s3Session, awsConfig), config: s3Config, closed: atomic.NewBool(false)}, nil
}

func (a *S3) UploadBytes(fileName string, fileBytes []byte) error {
	return a.Upload(fileName, bytes.NewReader(fileBytes))
}

// Upload creates named file on s3 with payload
func (a *S3) Upload(fileName string, fileReader io.ReadSeeker) error {
	fileName = a.Path(fileName)

	if a.closed.Load() {
		return fmt.Errorf("attempt to use closed S3 instance")
	}

	params := &s3.PutObjectInput{
		Bucket: aws.String(a.config.Bucket),
	}
	if a.config.Compression == types2.FileCompressionGZIP {
		params.ContentType = aws.String("application/gzip")
	} else {
		switch a.config.Format {
		case types2.FileFormatCSV:
			params.ContentType = aws.String("text/csv")
		case types2.FileFormatNDJSON, types2.FileFormatNDJSONFLAT:
			params.ContentType = aws.String("application/x-ndjson")
		}
	}
	params.Key = aws.String(fileName)
	params.Body = fileReader
	if _, err := a.client.PutObject(params); err != nil {
		return errorj.SaveOnStageError.Wrap(err, "failed to write file to s3").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Bucket:    a.config.Bucket,
				Statement: fmt.Sprintf("file: %s", fileName),
			})
	}
	return nil
}

// Download downloads file from s3 bucket
func (a *S3) Download(fileName string) ([]byte, error) {
	fileName = a.Path(fileName)

	if a.closed.Load() {
		return nil, fmt.Errorf("attempt to use closed S3 instance")
	}

	params := &s3.GetObjectInput{
		Bucket: aws.String(a.config.Bucket),
		Key:    aws.String(fileName),
	}
	resp, err := a.client.GetObject(params)
	if err != nil {
		return nil, errorj.SaveOnStageError.Wrap(err, "failed to read file from s3").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Bucket:    a.config.Bucket,
				Statement: fmt.Sprintf("file: %s", fileName),
			})
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errorj.SaveOnStageError.Wrap(err, "failed to read file from s3").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Bucket:    a.config.Bucket,
				Statement: fmt.Sprintf("file: %s", fileName),
			})
	}
	return data, nil
}

// DeleteObject deletes object from s3 bucket by key
func (a *S3) DeleteObject(key string) error {
	key = a.Path(key)

	if a.closed.Load() {
		return fmt.Errorf("attempt to use closed S3 instance")
	}
	input := &s3.DeleteObjectInput{Bucket: &a.config.Bucket, Key: &key}
	output, err := a.client.DeleteObject(input)
	if err != nil {
		return errorj.SaveOnStageError.Wrap(err, "failed to delete from s3").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Bucket:    a.config.Bucket,
				Statement: fmt.Sprintf("file: %s", key),
			})
	}

	if output != nil && output.DeleteMarker != nil && !*(output.DeleteMarker) {
		return errorj.SaveOnStageError.Wrap(err, "file hasn't been deleted from s3").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Bucket:    a.config.Bucket,
				Statement: fmt.Sprintf("file: %s", key),
			})
	}

	return nil
}

// ValidateWritePermission tries to create temporary file and remove it.
// returns nil if file creation was successful.
func (a *S3) ValidateWritePermission() error {
	filename := fmt.Sprintf("test_%v", time.Now())

	if err := a.UploadBytes(filename, []byte{}); err != nil {
		return err
	}

	if err := a.DeleteObject(filename); err != nil {
		logging.Warnf("Cannot remove object %q from S3: %v", filename, err)
		// Suppressing error because we need to check only write permission
		// return err
	}

	return nil
}

// Close returns nil
func (a *S3) Close() error {
	a.closed.Store(true)
	return nil
}
