package main

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"k8s.io/client-go/kubernetes"
	"net/http"
	"time"
)

type Context struct {
	config    *Config
	server    *http.Server
	clientset *kubernetes.Clientset
	manager   *Manager
}

func (a *Context) InitContext(settings *appbase.AppSettings) error {
	var err error
	a.config = &Config{}
	err = appbase.InitAppConfig(a.config, settings)
	if err != nil {
		return err
	}
	a.clientset, err = GetK8SClientSet(a)
	if err != nil {
		return err
	}
	a.manager = NewManager(a)

	router := NewRouter(a)
	a.server = &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", a.config.HTTPPort),
		Handler:           router.Engine(),
		ReadTimeout:       time.Second * 60,
		ReadHeaderTimeout: time.Second * 60,
		IdleTimeout:       time.Second * 65,
	}
	return nil
}

func (a *Context) Cleanup() error {
	return nil
}

func (a *Context) ShutdownSignal() error {
	_ = a.server.Shutdown(context.Background())
	return nil
}

func (a *Context) Server() *http.Server {
	return a.server
}

func (a *Context) Config() *Config {
	return a.config
}
