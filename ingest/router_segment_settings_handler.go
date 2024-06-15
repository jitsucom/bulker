package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/types"
	"net/http"
	"strings"
	"time"
)

var ids = types.NewSet[string]()

func init() {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for range ticker.C {
			logging.Infof("[SETTINGS]%s", strings.Join(ids.ToSlice(), ","))
		}
	}()
}

func (r *Router) SettingsHandler(c *gin.Context) {
	writeKey := c.Param("writeKey")
	ids.Put(strings.SplitN(writeKey, ":", 2)[0])
	c.Header("Cache-Control", "public, max-age=86400")
	c.Data(http.StatusOK, "application/json", []byte(fmt.Sprintf(`{
  "integrations": {
    "Actions Google Analytic 4": {
      "versionSettings": {
        "componentTypes": []
      }
    },
    "Segment.io": {
      "apiKey": "%s",
      "unbundledIntegrations": [],
      "addBundledMetadata": true,
      "maybeBundledConfigIds": {},
      "versionSettings": {
        "version": "4.4.7",
        "componentTypes": [
          "browser"
        ]
      }
    }
  },
  "plan": {
    "track": {
      "__default": {
        "enabled": true,
        "integrations": {}
      }
    },
    "identify": {
      "__default": {
        "enabled": true
      }
    },
    "group": {
      "__default": {
        "enabled": true
      }
    }
  },
  "edgeFunction": {},
  "analyticsNextEnabled": true,
  "middlewareSettings": {},
  "enabledMiddleware": {},
  "metrics": {
    "sampleRate": 0.1
  },
  "legacyVideoPluginsEnabled": false,
  "remotePlugins": []
}`, writeKey)))
}
