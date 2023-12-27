package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (r *Router) ScriptHandler(c *gin.Context) {
	if r.script == nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	if c.Request.Method != "GET" && c.Request.Method != "HEAD" {
		c.AbortWithStatus(http.StatusMethodNotAllowed)
		return
	}
	ifNoneMatch := c.GetHeader("If-None-Match")
	etag := r.script.GetEtag()
	if etag != nil && ifNoneMatch != "" && *etag == ifNoneMatch {
		c.Header("ETag", *etag)
		c.AbortWithStatus(http.StatusNotModified)
		return
	}
	r.script.WriteScript(c, c.Request.Method == "HEAD", r.ShouldCompress(c.Request))
}
