package app

import (
	"github.com/go-co-op/gocron/v2"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"math/rand"
	"time"
)

type Cron struct {
	appbase.Service
	config    *Config
	scheduler gocron.Scheduler
}

func NewCron(config *Config) *Cron {
	base := appbase.NewServiceBase("cron")
	s, _ := gocron.NewScheduler(gocron.WithLocation(time.UTC))
	s.Start()
	return &Cron{Service: base, scheduler: s, config: config}
}

func (c *Cron) AddBatchConsumer(batchConsumer BatchConsumer) (gocron.Job, error) {
	return c.scheduler.NewJob(gocron.DurationJob(time.Duration(batchConsumer.BatchPeriodSec())*time.Second),
		gocron.NewTask(batchConsumer.RunJob),
		gocron.WithStartAt(gocron.WithStartDateTime(time.Now().Add(time.Duration(rand.Intn(batchConsumer.BatchPeriodSec()))*time.Second))),
		gocron.WithTags(batchConsumer.TopicId()))
}

func (c *Cron) ReplaceBatchConsumer(batchConsumer BatchConsumer) (gocron.Job, error) {
	c.RemoveBatchConsumer(batchConsumer)
	return c.AddBatchConsumer(batchConsumer)
}

func (c *Cron) RemoveBatchConsumer(batchConsumer BatchConsumer) {
	c.scheduler.RemoveByTags(batchConsumer.TopicId())
}

// Close scheduler
func (c *Cron) Close() {
	stopped := make(chan struct{})
	go func() {
		_ = c.scheduler.Shutdown()
		close(stopped)
	}()
	select {
	case <-stopped:
		c.Infof("Cron scheduler stopped")
	case <-time.After(time.Duration(c.config.ShutdownTimeoutSec) * time.Second):
		c.Warnf("Shutdown timeout [%ds] expired. Scheduler will be stopped forcibly. Active batches may be interrupted abruptly", c.config.ShutdownTimeoutSec)
	}
}
