package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
)

func (r *Router) SettingsHandler(c *gin.Context) {
	writeKey := c.Param("writeKey")
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
