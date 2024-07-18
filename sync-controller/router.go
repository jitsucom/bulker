package main

import (
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"net/http"
	"net/http/pprof"
)

type Router struct {
	*appbase.Router
}

func NewRouter(appContext *Context) *Router {
	base := appbase.NewRouterBase(appContext.config.Config, []string{"/health"})

	router := &Router{
		Router: base,
	}
	engine := router.Engine()
	engine.GET("/spec", appContext.taskManager.SpecHandler)
	engine.POST("/check", appContext.taskManager.CheckHandler)
	engine.POST("/discover", appContext.taskManager.DiscoverHandler)
	engine.POST("/read", appContext.taskManager.ReadHandler)
	engine.GET("/cancel", appContext.taskManager.CancelHandler)

	engine.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "pass"})
	})

	engine.GET("/debug/pprof/profile", gin.WrapF(pprof.Profile))
	engine.GET("/debug/pprof/heap", gin.WrapF(pprof.Handler("heap").ServeHTTP))
	engine.GET("/debug/pprof/goroutine", gin.WrapF(pprof.Handler("goroutine").ServeHTTP))
	engine.GET("/debug/pprof/block", gin.WrapF(pprof.Handler("block").ServeHTTP))
	engine.GET("/debug/pprof/threadcreate", gin.WrapF(pprof.Handler("threadcreate").ServeHTTP))
	engine.GET("/debug/pprof/cmdline", gin.WrapF(pprof.Handler("cmdline").ServeHTTP))
	engine.GET("/debug/pprof/symbol", gin.WrapF(pprof.Handler("symbol").ServeHTTP))
	engine.GET("/debug/pprof/trace", gin.WrapF(pprof.Handler("trace").ServeHTTP))
	engine.GET("/debug/pprof/mutex", gin.WrapF(pprof.Handler("mutex").ServeHTTP))
	engine.GET("/debug/pprof", gin.WrapF(pprof.Index))

	return router
}
