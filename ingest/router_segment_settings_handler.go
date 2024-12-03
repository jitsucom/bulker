package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

const settingsHeader = "{\"integrations\":{\"Segment.io\":{\"apiKey\":\""
const settingsFooter = "\",\"versionSettings\":{\"version\":\"4.4.7\",\"componentTypes\":[\"browser\"]}}},\"plan\":{\"track\":{\"__default\":{\"enabled\":true}},\"identify\":{\"__default\":{\"enabled\":true}},\"group\":{\"__default\":{\"enabled\":true}}},\"analyticsNextEnabled\":true}"

func (r *Router) SettingsHandler(c *gin.Context) {
	writeKey := c.Param("writeKey")
	writer := c.Writer
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "public, max-age=86400")
	writer.WriteHeader(200)
	_, err := writer.WriteString(settingsHeader)
	if err != nil {
		r.ResponseError(c, http.StatusBadRequest, "error writing response", false, err, true, true)
		return
	}
	_, err = writer.WriteString(writeKey)
	if err != nil {
		r.ResponseError(c, http.StatusBadRequest, "error writing response", false, err, true, true)
		return
	}
	_, err = writer.WriteString(settingsFooter)
	if err != nil {
		r.ResponseError(c, http.StatusBadRequest, "error writing response", false, err, true, true)
		return
	}
}
