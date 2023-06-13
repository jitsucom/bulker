package main

import (
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"net/http"
)

type TaskManager struct {
	appbase.Service
	config    *Config
	jobRunner *JobRunner
	dbpool    *pgxpool.Pool
	closeCh   chan struct{}
}

func NewTaskManager(appContext *Context) (*TaskManager, error) {
	base := appbase.NewServiceBase("task-manager")

	t := &TaskManager{Service: base, config: appContext.config, jobRunner: appContext.jobRunner, dbpool: appContext.dbpool,
		closeCh: make(chan struct{})}
	return t, nil
}

func (t *TaskManager) SpecHandler(c *gin.Context) {
	image := c.Query("image")
	version := c.Query("version")
	taskDescriptor := TaskDescriptor{
		TaskType:       "spec",
		Package:        image,
		PackageVersion: version,
	}

	taskStatus := t.jobRunner.CreatePod(taskDescriptor, nil)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Description})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (t *TaskManager) CheckHandler(c *gin.Context) {
	image := c.Query("image")
	version := c.Query("version")
	taskConfig := TaskConfiguration{}
	err := c.BindJSON(&taskConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}
	taskDescriptor := TaskDescriptor{
		TaskType:       "check",
		Package:        image,
		PackageVersion: version,
	}

	taskStatus := t.jobRunner.CreatePod(taskDescriptor, &taskConfig)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Description})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (t *TaskManager) DiscoverHandler(c *gin.Context) {
	image := c.Query("image")
	version := c.Query("version")
	taskConfig := TaskConfiguration{}
	err := c.BindJSON(&taskConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}
	taskDescriptor := TaskDescriptor{
		TaskType:       "discover",
		Package:        image,
		PackageVersion: version,
	}

	taskStatus := t.jobRunner.CreatePod(taskDescriptor, &taskConfig)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Description})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (t *TaskManager) ReadHandler(c *gin.Context) {
	image := c.Query("image")
	version := c.Query("version")
	destinationId := c.Query("destinationId")
	taskConfig := TaskConfiguration{}
	err := c.BindJSON(&taskConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}
	if taskConfig.State == nil {
		taskConfig.State = map[string]any{}
	}
	taskDescriptor := TaskDescriptor{
		TaskType:       "read",
		Package:        image,
		PackageVersion: version,
		DestinationId:  destinationId,
	}

	taskStatus := t.jobRunner.CreatePod(taskDescriptor, &taskConfig)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Description})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}
