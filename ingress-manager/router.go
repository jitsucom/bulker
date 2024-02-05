package main

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"net/http"
	"net/http/pprof"
)

type Router struct {
	*appbase.Router
	manager *Manager
}

func NewRouter(appContext *Context) *Router {
	base := appbase.NewRouterBase(appContext.config.Config, []string{"/health"})

	router := &Router{
		Router:  base,
		manager: appContext.manager,
	}
	engine := router.Engine()
	engine.GET("/api/domain", router.DomainHandler)
	engine.POST("/api/domain", router.DomainsHandler)

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

func (r *Router) DomainHandler(c *gin.Context) {
	domain := c.Query("domain")
	if domain == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "domain is required"})
		return
	}
	status, err := r.manager.AddDomain(domain)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": status})
}

type DomainsPayload struct {
	Domains []string `json:"domains"`
}

func (r *Router) DomainsHandler(c *gin.Context) {
	payload := DomainsPayload{}
	err := json.NewDecoder(c.Request.Body).Decode(&payload)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result := map[string]map[string]any{}

	for _, domain := range payload.Domains {
		status, err := r.manager.AddDomain(domain)
		if err != nil {
			result[domain] = map[string]any{"status": status, "error": err.Error()}
			return
		} else {
			result[domain] = map[string]any{"status": status}
		}
	}

	c.JSON(http.StatusOK, result)
}
