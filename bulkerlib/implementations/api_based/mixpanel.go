package api_based

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

const MixpanelBulkerTypeId = "mixpanel"
const MixpanelUnsupported = "Only 'batch' mode is supported"

func init() {
	bulker.RegisterBulker(MixpanelBulkerTypeId, NewMixpanelBulker)
}

type MixpanelConfig struct {
	ProjectId              string `mapstructure:"projectId" json:"projectId" yaml:"projectId"`
	ServiceAccountUserName string `mapstructure:"serviceAccountUserName" json:"serviceAccountUserName" yaml:"serviceAccountUserName"`
	ServiceAccountPassword string `mapstructure:"serviceAccountPassword" json:"serviceAccountPassword" yaml:"serviceAccountPassword"`
}
type MixpanelBulker struct {
	appbase.Service
	config     MixpanelConfig
	httpClient *http.Client

	closed *atomic.Bool
}

func NewMixpanelBulker(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	fmt.Println("Init MixpanelBulker")
	mixpanelConfig := MixpanelConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, &mixpanelConfig); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %v", err)
	}
	httpClient := &http.Client{
		Timeout: time.Duration(5) * time.Second,
	}
	return &MixpanelBulker{Service: appbase.NewServiceBase(MixpanelBulkerTypeId), config: mixpanelConfig, httpClient: httpClient,
		closed: &atomic.Bool{}}, nil
}

func (mp *MixpanelBulker) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	switch mode {
	case bulker.Stream:
		return nil, errors.New(MixpanelUnsupported)
	case bulker.Batch:
		return NewTransactionalStream(id, mp, streamOptions...)
	case bulker.ReplaceTable:
		return nil, errors.New(MixpanelUnsupported)
	case bulker.ReplacePartition:
		return nil, errors.New(MixpanelUnsupported)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}

func (mp *MixpanelBulker) Type() string {
	return MixpanelBulkerTypeId
}

func (mp *MixpanelBulker) Upload(fileReader io.ReadSeeker) (string, error) {
	if mp.closed.Load() {
		return "", fmt.Errorf("attempt to use closed Mixpanel instance")
	}
	buff := bytes.Buffer{}
	gz := gzip.NewWriter(&buff)
	buf := bufio.NewWriter(gz)
	_, _ = buf.WriteRune('[')
	scanner := bufio.NewScanner(fileReader)
	scanner.Buffer(make([]byte, 1024*10), 1024*1024*1)
	first := true
	for scanner.Scan() {
		if !first {
			_, _ = buf.WriteRune(',')
		} else {
			first = false
		}
		_, _ = buf.WriteString(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		mp.Errorf("Upload: failed to read file: %v", err)
	} else {
		_, _ = buf.WriteRune(']')
	}
	err := buf.Flush()
	if err != nil {
		mp.Errorf("Upload: failed to flush buffered writer: %v", err)
	}
	err = gz.Close()
	if err != nil {
		mp.Errorf("Upload: failed to close gzip writer: %v", err)
	}

	req, err := http.NewRequest("POST", "https://api.mixpanel.com/import?strict=1&project_id="+mp.config.ProjectId, &buff)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	serviceAccount := fmt.Sprintf("%s:%s", mp.config.ServiceAccountUserName, mp.config.ServiceAccountPassword)
	req.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(serviceAccount))))

	res, err := mp.httpClient.Do(req)
	if err != nil {
		return "", err
	} else {
		defer res.Body.Close()
		//get body
		var body []byte
		body, err = io.ReadAll(res.Body)
		if res.StatusCode != 200 || err != nil {
			return "", mp.NewError("Failed to send Mixpanel batch: status: %v body: %s", res.StatusCode, string(body))
		} else {
			return string(body), nil
		}
	}

}
func (mp *MixpanelBulker) Close() error {
	mp.closed.Store(true)
	mp.httpClient.CloseIdleConnections()
	return nil
}
