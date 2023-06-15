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
	engine.GET("/spec", appContext.taskManager.SpecHandler)
	engine.POST("/check", appContext.taskManager.CheckHandler)
	engine.POST("/discover", appContext.taskManager.DiscoverHandler)
	engine.POST("/read", appContext.taskManager.ReadHandler)

	engine.GET("/health", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	return router
}
