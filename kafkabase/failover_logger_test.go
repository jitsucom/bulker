package kafkabase

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailoverLogger_LogPayload(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "failover_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := &FailoverLoggerConfig{
		Enabled:          true,
		BasePath:         tempDir,
		RotationPeriod:   time.Hour,
		MaxSize:          1024 * 1024, // 1MB
		CompressOnRotate: false,
	}

	logger, err := NewFailoverLogger(config)
	require.NoError(t, err)
	require.NotNil(t, logger)
	defer logger.Close()

	// Create test payload
	payload := []byte(`{"test": "data"}`)

	// Log payload
	err = logger.LogPayload(payload)
	assert.NoError(t, err)
	logger.Close()
	// Read file and verify
	files, err := filepath.Glob(filepath.Join(tempDir, "*.ndjson"))
	require.NoError(t, err)
	require.Len(t, files, 1)

	data, err := os.ReadFile(files[0])
	require.NoError(t, err)

	// Should be raw payload + newline
	assert.Equal(t, string(payload)+"\n", string(data))
}


func TestFailoverLogger_Rotation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "failover_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Small max size to trigger rotation
	config := &FailoverLoggerConfig{
		Enabled:          true,
		BasePath:         tempDir,
		RotationPeriod:   time.Hour,
		MaxSize:          100, // Very small to trigger rotation
		CompressOnRotate: true,
	}

	logger, err := NewFailoverLogger(config)
	require.NoError(t, err)
	defer logger.Close()
	logger.Start()
	// Log multiple payloads to trigger rotation
	for i := 0; i < 20; i++ {
		payload := []byte(`{"data": "test message with some content to fill space"}`)
		err = logger.LogPayload(payload)
		assert.NoError(t, err)
		time.Sleep(1 * time.Second)
	}

	// Wait for rotation to complete
	time.Sleep(20 * time.Second)

	// Check for compressed files
	gzFiles, err := filepath.Glob(filepath.Join(tempDir, "*.gz"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(gzFiles), 1, "Should have at least one compressed file")
}

func TestLocalFileDestination_MaxOldFiles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "failover_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dest := NewLocalFileDestination(tempDir, 2, true)

	// Create multiple files to move
	for i := 0; i < 5; i++ {
		// Create temp file first
		tempFile := filepath.Join(tempDir, fmt.Sprintf("temp_test_%d.ndjson.gz", i))
		err = os.WriteFile(tempFile, []byte("test data"), 0644)
		require.NoError(t, err)

		// Move it using Store
		err = dest.Store(tempFile)
		assert.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Small delay to ensure different mod times
	}

	// Check that only maxOldFiles remain
	files, err := filepath.Glob(filepath.Join(tempDir, "*.gz"))
	require.NoError(t, err)
	assert.LessOrEqual(t, len(files), 2, "Should have at most 2 files")
}

func TestFailoverLoggerConfig_Disabled(t *testing.T) {
	config := &FailoverLoggerConfig{
		Enabled: false,
	}

	logger, err := NewFailoverLogger(config)
	assert.NoError(t, err)
	assert.Nil(t, logger)
}

func TestFailoverLogger_ShouldLog(t *testing.T) {
	// Test error-only mode (default)
	config := &FailoverLoggerConfig{
		Enabled:        true,
		LogAllMessages: false,
	}
	logger := &FailoverLogger{config: config}

	// Should log when there's a non-Kafka error
	testError := fmt.Errorf("some error")
	assert.True(t, logger.ShouldLog(testError))

	// Should not log when there's no error
	assert.False(t, logger.ShouldLog(nil))
	
	// Should not log non-retriable Kafka errors
	nonRetriableError := kafka.NewError(kafka.ErrMsgSizeTooLarge, "Message size too large", false)
	assert.False(t, logger.ShouldLog(nonRetriableError))
	
	// Should log non-Kafka errors (they are treated as retriable)
	genericError := fmt.Errorf("generic network error")
	assert.True(t, logger.ShouldLog(genericError))
}

func TestFailoverLogger_ShouldLog_AllMessages(t *testing.T) {
	// Test all-messages mode
	config := &FailoverLoggerConfig{
		Enabled:        true,
		LogAllMessages: true,
	}
	logger := &FailoverLogger{config: config}

	// Should log regardless of error status
	testError := fmt.Errorf("some error")
	assert.True(t, logger.ShouldLog(testError))
	assert.True(t, logger.ShouldLog(nil))
	
	// Should log even "Message size too large" errors when LogAllMessages is true
	sizeError := fmt.Errorf("Broker: Message size too large")
	assert.True(t, logger.ShouldLog(sizeError))
}
