package app

import (
	"github.com/go-co-op/gocron"
	"github.com/jitsucom/bulker/base/objects"
	"math/rand"
	"time"
)

type Cron struct {
	objects.ServiceBase
	config    *AppConfig
	scheduler *gocron.Scheduler
}

func NewCron(config *AppConfig) *Cron {
	base := objects.NewServiceBase("cron")
	s := gocron.NewScheduler(time.UTC)
	s.TagsUnique()
	s.StartAsync()
	return &Cron{ServiceBase: base, scheduler: s, config: config}
}

func (c *Cron) AddBatchConsumer(batchConsumer *BatchConsumer) (*gocron.Job, error) {
	batchPeriodSeconds := batchConsumer.batchPeriodSec
	if batchPeriodSeconds == 0 {
		batchPeriodSeconds = c.config.BatchRunnerPeriodSec
	}
	return c.scheduler.Every(batchPeriodSeconds).Seconds().
		StartAt(time.Now().Add(time.Duration(rand.Intn(batchPeriodSeconds)) * time.Second)).
		Tag(batchConsumer.topicId).
		Do(batchConsumer.RunJob)
}

func (c *Cron) ReplaceBatchConsumer(batchConsumer *BatchConsumer) (*gocron.Job, error) {
	_ = c.scheduler.RemoveByTag(batchConsumer.topicId)
	batchPeriodSeconds := batchConsumer.batchPeriodSec
	if batchPeriodSeconds == 0 {
		batchPeriodSeconds = c.config.BatchRunnerPeriodSec
	}
	return c.scheduler.Every(batchPeriodSeconds).Seconds().
		StartAt(time.Now().Add(time.Duration(rand.Intn(batchPeriodSeconds)) * time.Second)).
		Tag(batchConsumer.topicId).
		Do(batchConsumer.RunJob)
}

// Close scheduler
func (c *Cron) Close() {
	stopped := make(chan struct{})
	go func() {
		c.scheduler.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Duration(c.config.ShutdownTimeoutSec) * time.Second):
		c.Warnf("Shutdown timeout [%ds] expired. Scheduler will be stopped forcibly. Active batches may be interrupted abruptly", c.config.ShutdownTimeoutSec)
	}
}
