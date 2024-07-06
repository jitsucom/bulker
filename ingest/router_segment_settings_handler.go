package main

import (
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"net/http"
	"strings"
	"sync"
	"time"
)

const settingsHeader = "{\"integrations\":{\"Segment.io\":{\"apiKey\":\""
const settingsFooter = "\",\"unbundledIntegrations\":[],\"addBundledMetadata\":true,\"maybeBundledConfigIds\":{},\"versionSettings\":{\"version\":\"4.4.7\",\"componentTypes\":[\"browser\"]}}},\"plan\":{\"track\":{\"__default\":{\"enabled\":true,\"integrations\":{}}},\"identify\":{\"__default\":{\"enabled\":true}},\"group\":{\"__default\":{\"enabled\":true}}},\"edgeFunction\":{},\"analyticsNextEnabled\":true,\"middlewareSettings\":{},\"enabledMiddleware\":{},\"metrics\":{\"sampleRate\":0.1},\"legacyVideoPluginsEnabled\":false,\"remotePlugins\":[]}"

var ids = sync.Map{}

func init() {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for range ticker.C {
			arr := make([]string, 0)
			ids.Range(func(key, value interface{}) bool {
				arr = append(arr, key.(string))
				return true
			})
			logging.Infof("[SETTINGS]%s", strings.Join(arr, ","))
		}
	}()
}

func (r *Router) SettingsHandler(c *gin.Context) {
	writeKey := c.Param("writeKey")
	ids.LoadOrStore(strings.SplitN(writeKey, ":", 2)[0], true)
	writer := c.Writer
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "public, max-age=86400")
	writer.WriteHeader(200)
	_, err := writer.WriteString(settingsHeader)
	if err != nil {
		r.ResponseError(c, http.StatusBadRequest, "error writing response", false, err, true)
		return
	}
	_, err = writer.WriteString(writeKey)
	if err != nil {
		r.ResponseError(c, http.StatusBadRequest, "error writing response", false, err, true)
		return
	}
	_, err = writer.WriteString(settingsFooter)
	if err != nil {
		r.ResponseError(c, http.StatusBadRequest, "error writing response", false, err, true)
		return
	}
}
