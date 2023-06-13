package app

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"
)

type MetricsServer struct {
	appbase.Service
	server *http.Server
}

func NewMetricsServer(appconfig *Config) *MetricsServer {
	base := appbase.NewServiceBase("metrics_server")
	engine := gin.New()
	engine.Use(gin.Recovery())
	//expose prometheus metrics
	engine.GET("/metrics", gin.WrapH(promhttp.Handler()))

	server := &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", appconfig.MetricsPort),
		Handler:           engine,
		ReadTimeout:       time.Second * 60,
		ReadHeaderTimeout: time.Second * 60,
		IdleTimeout:       time.Second * 65,
	}
	m := &MetricsServer{Service: base, server: server}
	m.start()
	return m
}

func (s *MetricsServer) start() {
	go func() {
		s.Infof("Starting metrics server on %s", s.server.Addr)
		s.Infof("%v", s.server.ListenAndServe())
	}()
}

func (s *MetricsServer) Stop() error {
	s.Infof("Stopping metrics server")
	return s.server.Shutdown(context.Background())
}
