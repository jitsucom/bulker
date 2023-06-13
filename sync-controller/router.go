package main

import (
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"net/http"
	"strings"
)

type Router struct {
	*appbase.Router
}

func NewRouter(appContext *Context) *Router {
	authTokens := strings.Split(appContext.config.AuthTokens, ",")
	tokenSecrets := strings.Split(appContext.config.TokenSecrets, ",")
	base := appbase.NewRouterBase(authTokens, tokenSecrets, []string{"/health"})

	router := &Router{
		Router: base,
	}
	engine := router.Engine()
	engine.GET("/source/spec", appContext.taskManager.SpecHandler)
	engine.POST("/source/check", appContext.taskManager.CheckHandler)
	engine.POST("/source/discover", appContext.taskManager.DiscoverHandler)
	engine.POST("/source/read", appContext.taskManager.ReadHandler)

	engine.GET("/health", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	return router
}
