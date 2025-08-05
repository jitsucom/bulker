package main

import (
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/appbase"
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
		"/admin",
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
	engine.GET("/admin", router.serveAdminHTML)
	engine.Static("/static", "./static")

	return router

}
