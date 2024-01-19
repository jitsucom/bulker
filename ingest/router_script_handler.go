package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
)

func (r *Router) ScriptHandler(c *gin.Context) {
	if c.Request.Method != "GET" && c.Request.Method != "HEAD" {
		c.AbortWithStatus(http.StatusMethodNotAllowed)
		return
	}
	if r.scriptRepository == nil {
		_ = c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("Script repository is absent"))
		return
	}
	script := r.scriptRepository.GetData()
	if script == nil {
		_ = c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("Script repository is not initialized"))
		return
	}

	ifNoneMatch := c.GetHeader("If-None-Match")
	etag := script.GetEtag()
	if etag != "" {
		c.Header("ETag", etag)
		if ifNoneMatch != "" && etag == ifNoneMatch {
			c.AbortWithStatus(http.StatusNotModified)
			return
		}
	}

	script.WriteScript(c, c.Request.Method == "HEAD", r.ShouldCompress(c.Request))
}
