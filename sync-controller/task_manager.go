package main

import (
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/sync-sidecar/db"
	"strings"
	"time"

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
	go t.listenTaskStatus()
	return t, nil
}

func (t *TaskManager) SpecHandler(c *gin.Context) {
	image := c.Query("package")
	version := c.Query("version")
	startedAt := time.Now().Round(time.Second)
	taskDescriptor := TaskDescriptor{
		TaskType:       "spec",
		Package:        image,
		PackageVersion: version,
		StartedAt:      startedAt.Format(time.RFC3339),
	}

	taskStatus := t.jobRunner.CreatePod(taskDescriptor, nil)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Description})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "startedAt": startedAt.Unix()})
}

func (t *TaskManager) CheckHandler(c *gin.Context) {
	taskConfig := TaskConfiguration{}
	err := c.BindJSON(&taskConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}
	taskDescriptor := TaskDescriptor{
		TaskType:       "check",
		Package:        c.Query("package"),
		PackageVersion: c.Query("version"),
		StorageKey:     c.Query("storageKey"),
		StartedAt:      time.Now().Format(time.RFC3339),
	}

	taskStatus := t.jobRunner.CreatePod(taskDescriptor, &taskConfig)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Description})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (t *TaskManager) DiscoverHandler(c *gin.Context) {
	taskConfig := TaskConfiguration{}
	err := c.BindJSON(&taskConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}
	taskDescriptor := TaskDescriptor{
		TaskType:       "discover",
		Package:        c.Query("package"),
		PackageVersion: c.Query("version"),
		StorageKey:     c.Query("storageKey"),
		StartedAt:      time.Now().Format(time.RFC3339),
	}

	taskStatus := t.jobRunner.CreatePod(taskDescriptor, &taskConfig)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Description})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (t *TaskManager) ReadHandler(c *gin.Context) {
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
		Package:        c.Query("package"),
		PackageVersion: c.Query("version"),
		SyncID:         c.Query("syncId"),
		TaskID:         c.Query("taskId"),
		StartedAt:      time.Now().Format(time.RFC3339),
	}

	taskStatus := t.jobRunner.CreatePod(taskDescriptor, &taskConfig)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Description})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (t *TaskManager) listenTaskStatus() {
	for {
		select {
		case <-t.closeCh:
			return
		case st := <-t.jobRunner.TaskStatusChannel():
			var err error
			switch st.TaskType {
			case "spec":
				if st.Status == StatusCreateFailed || st.Status == StatusFailed || st.Status == StatusInitTimeout {
					err = db.UpsertSpec(t.dbpool, st.Package, st.PackageVersion, nil, st.StartedAtTime(), st.Description)
				}
			case "discover":
				if st.Status == StatusCreateFailed || st.Status == StatusFailed || st.Status == StatusInitTimeout {
					err = db.UpsertCatalog(t.dbpool, st.Package, st.PackageVersion, st.StorageKey, nil, st.StartedAtTime(), "FAILED", st.Description)
				} else if st.Status == StatusCreated {
					err = db.UpsertCatalog(t.dbpool, st.Package, st.PackageVersion, st.StorageKey, nil, st.StartedAtTime(), "RUNNING", st.Description)
				}
			case "check":
				if st.Status == StatusCreateFailed || st.Status == StatusFailed || st.Status == StatusInitTimeout {
					err = db.UpsertCheck(t.dbpool, st.Package, st.PackageVersion, st.StorageKey, "FAILED", strings.Join([]string{string(st.Status), st.Description}, ": "), st.StartedAtTime())
				}
			case "read":
				switch st.Status {
				case StatusCreateFailed, StatusFailed, StatusInitTimeout:
					err = db.UpsertRunningTask(t.dbpool, st.SyncID, st.TaskID, st.Package, st.PackageVersion, st.StartedAtTime(), "FAILED", strings.Join([]string{string(st.Status), st.Description}, ": "))
				case StatusCreated:
					err = db.UpsertRunningTask(t.dbpool, st.SyncID, st.TaskID, st.Package, st.PackageVersion, st.StartedAtTime(), "RUNNING", strings.Join([]string{string(st.Status), st.Description}, ": "))
				default:
					//do nothing. sidecar manages success status.
				}
			}
			if err != nil {
				t.Errorf("Unable to update '%s' status: %v\n", st.TaskType, err)
			}
			if st.Status != StatusPending {
				t.Infof("taskStatus: %+v\n", *st)
			} else {
				t.Debugf("taskStatus: %+v\n", *st)
			}
		}
	}
}

func (t *TaskManager) Close() {
	select {
	case <-t.closeCh:
	default:
		close(t.closeCh)
	}
}
