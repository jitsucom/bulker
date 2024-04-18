package api_based

import (
	"encoding/base64"
	"errors"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
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

func (mp *MixpanelBulker) Upload(reader io.Reader) (string, error) {
	if mp.closed.Load() {
		return "", fmt.Errorf("attempt to use closed Mixpanel instance")
	}

	req, err := http.NewRequest("POST", "https://api.mixpanel.com/import?strict=1&project_id="+mp.config.ProjectId, reader)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
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

func (mp *MixpanelBulker) GetBatchFileFormat() types2.FileFormat {
	return types2.FileFormatNDJSON
}
func (mp *MixpanelBulker) GetBatchFileCompression() types2.FileCompression {
	return types2.FileCompressionGZIP
}

func (mp *MixpanelBulker) InmemoryBatch() bool {
	return true
}

func (mp *MixpanelBulker) Close() error {
	mp.closed.Store(true)
	mp.httpClient.CloseIdleConnections()
	return nil
}
