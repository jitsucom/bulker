package main

import (
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"

	"net/http"
)

type Router struct {
	*appbase.Router
	context             *Context
	reprocessingManager *ReprocessingJobManager
}

func NewRouter(context *Context) *Router {
	base := appbase.NewRouterBase(context.config.Config, []string{
		"/health",
		"/",
		"/static",
	})
	router := &Router{
		Router:              base,
		context:             context,
		reprocessingManager: context.reprocessingManager,
	}
	engine := router.Engine()
	engine.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Admin API routes (protected by auth)
	adminAPI := engine.Group("/api/admin")
	reprocessingAPI := adminAPI.Group("/reprocessing")

	reprocessingAPI.POST("/jobs", router.startReprocessingJob)
	reprocessingAPI.GET("/jobs", router.listReprocessingJobs)
	reprocessingAPI.GET("/jobs/:id", router.getReprocessingJob)
	reprocessingAPI.POST("/jobs/:id/pause", router.pauseReprocessingJob)
	reprocessingAPI.POST("/jobs/:id/resume", router.resumeReprocessingJob)
	reprocessingAPI.POST("/jobs/:id/cancel", router.cancelReprocessingJob)

	// Serve HTML interface for admin (no auth required for UI, auth handled via form)
	engine.GET("/", router.serveAdminHTML)
	engine.Static("/static", "./static")

	return router

}
func (r *Router) CorsMiddleware(c *gin.Context) {
	origin := c.GetHeader("Origin")
	if c.Request.Method == "OPTIONS" {
		c.Header("Access-Control-Allow-Origin", utils.NvlString(origin, "*"))
		c.Header("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
		// x-jitsu-custom - in case client want to add some custom payload via header
		c.Header("Access-Control-Allow-Headers", "x-enable-debug, x-write-key, authorization, content-type, x-ip-policy, cache-control, x-jitsu-custom")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Max-Age", "86400")
		c.AbortWithStatus(http.StatusOK)
		return
	} else if origin != "" {
		c.Header("Access-Control-Allow-Origin", origin)
		c.Header("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
		// x-jitsu-custom - in case client want to add some custom payload via header
		c.Header("Access-Control-Allow-Headers", "x-enable-debug, x-write-key, authorization, content-type, x-ip-policy, cache-control, x-jitsu-custom")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Max-Age", "86400")
	}
	c.Next()
}
