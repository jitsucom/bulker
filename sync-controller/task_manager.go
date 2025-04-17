package main

import (
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/safego"
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
	safego.RunWithRestart(t.listenTaskStatus)
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

	taskStatus := t.jobRunner.CreateJob(taskDescriptor, nil)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Error})
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

	taskStatus := t.jobRunner.CreateJob(taskDescriptor, &taskConfig)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Error})
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

	taskStatus := t.jobRunner.CreateJob(taskDescriptor, &taskConfig)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Error})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (t *TaskManager) CancelHandler(c *gin.Context) {
	pkg := c.Query("package")
	syncId := c.Query("syncId")
	taskId := c.Query("taskId")
	_ = db.UpdateRunningTaskStatus(t.dbpool, taskId, "CANCELLED")
	t.jobRunner.TerminatePod(PodName(syncId, taskId, pkg))
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (t *TaskManager) ReadHandler(c *gin.Context) {
	taskConfig := TaskConfiguration{}
	err := jsonorder.NewDecoder(c.Request.Body).Decode(&taskConfig)
	defer c.Request.Body.Close()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}
	if taskConfig.State == nil {
		taskConfig.State = map[string]any{}
	}
	taskDescriptor := TaskDescriptor{
		TaskType:        "read",
		Package:         c.Query("package"),
		PackageVersion:  c.Query("version"),
		SyncID:          c.Query("syncId"),
		TaskID:          c.Query("taskId"),
		Namespace:       c.Query("namespace"),
		TableNamePrefix: c.Query("tableNamePrefix"),
		ToSameCase:      c.Query("toSameCase"),
		AddMeta:         c.Query("addMeta"),
		FullSync:        c.Query("fullSync"),
		Debug:           c.Query("debug"),
		StartedBy:       c.Query("startedBy"),
		StartedAt:       time.Now().Format(time.RFC3339),
	}

	taskStatus := t.jobRunner.CreateJob(taskDescriptor, &taskConfig)
	if taskStatus.Status == StatusCreateFailed {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Error})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

//
//func (t *TaskManager) ScheduleHandler(c *gin.Context) {
//	taskConfig := TaskConfiguration{}
//	err := jsonorder.NewDecoder(c.Request.Body).Decode(&taskConfig)
//	defer c.Request.Body.Close()
//	if err != nil {
//		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
//		return
//	}
//	if taskConfig.State == nil {
//		taskConfig.State = map[string]any{}
//	}
//	taskDescriptor := TaskDescriptor{
//		TaskType:        "read",
//		Package:         c.Query("package"),
//		PackageVersion:  c.Query("version"),
//		SyncID:          c.Query("syncId"),
//		TaskID:          c.Query("taskId"),
//		TableNamePrefix: c.Query("tableNamePrefix"),
//		StartedBy:       c.Query("startedBy"),
//		StartedAt:       time.Now().Format(time.RFC3339),
//	}
//
//	taskStatus := t.jobRunner.CreateCronJob(taskDescriptor, &taskConfig)
//	if taskStatus.Status == StatusCreateFailed {
//		c.JSON(http.StatusOK, gin.H{"ok": false, "error": taskStatus.Description})
//		return
//	}
//	c.JSON(http.StatusOK, gin.H{"ok": true})
//}

func (t *TaskManager) listenTaskStatus() {
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-t.closeCh:
			return
		case <-ticker.C:
			err := db.CloseStaleTasks(t.dbpool, time.Now().Add(-time.Hour))
			if err != nil {
				t.Errorf("Unable to close stale tasks: %v", err)
			}
		case st := <-t.jobRunner.TaskStatusChannel():
			var err error
			switch st.TaskType {
			case "spec":
				if st.Status == StatusCreateFailed || st.Status == StatusFailed || st.Status == StatusInitTimeout {
					err = db.InsertSpecError(t.dbpool, st.Package, st.PackageVersion, st.StartedAtTime(), st.Error)
				}
			case "discover":
				if st.Status == StatusCreateFailed || st.Status == StatusFailed || st.Status == StatusInitTimeout {
					err = db.UpsertRunningCatalogStatus(t.dbpool, st.Package, st.PackageVersion, st.StorageKey, st.StartedAtTime(), "FAILED", st.Error)
				} else if st.Status == StatusCreated {
					err = db.UpsertCatalogStatus(t.dbpool, st.Package, st.PackageVersion, st.StorageKey, st.StartedAtTime(), "RUNNING", "")
				}
			case "check":
				if st.Status == StatusCreateFailed || st.Status == StatusFailed || st.Status == StatusInitTimeout {
					err = db.InsertCheckError(t.dbpool, st.Package, st.PackageVersion, st.StorageKey, "FAILED", strings.Join([]string{string(st.Status), st.Error}, ": "), st.StartedAtTime())
				}
			case "read":
				switch st.Status {
				case StatusCreateFailed, StatusFailed, StatusInitTimeout:
					err = db.UpsertRunningTask(t.dbpool, st.SyncID, st.TaskID, st.Package, st.PackageVersion, st.StartedAtTime(), "FAILED", strings.Join([]string{string(st.Status), st.Error}, ": "), st.StartedBy)
				case StatusCreated:
					err = db.UpsertRunningTask(t.dbpool, st.SyncID, st.TaskID, st.Package, st.PackageVersion, st.StartedAtTime(), "RUNNING", "", st.StartedBy)
				case StatusRunning:
					if len(st.Metrics) > 0 {
						err = db.UpdateRunningTaskMetrics(t.dbpool, st.TaskID, st.Metrics)
					} else {
						err = db.UpdateRunningTaskDate(t.dbpool, st.TaskID)
					}
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
