package app

import (
	"github.com/go-co-op/gocron"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"math/rand"
	"time"
)

type Cron struct {
	appbase.Service
	config    *Config
	scheduler *gocron.Scheduler
}

func NewCron(config *Config) *Cron {
	base := appbase.NewServiceBase("cron")
	s := gocron.NewScheduler(time.UTC)
	s.TagsUnique()
	s.StartAsync()
	s.SingletonModeAll()
	return &Cron{Service: base, scheduler: s, config: config}
}

func (c *Cron) AddBatchConsumer(batchConsumer BatchConsumer) (*gocron.Job, error) {
	return c.scheduler.Every(batchConsumer.BatchPeriodSec()).Seconds().
		StartAt(time.Now().Add(time.Duration(rand.Intn(batchConsumer.BatchPeriodSec())) * time.Second)).
		Tag(batchConsumer.TopicId()).
		Do(batchConsumer.RunJob)
}

func (c *Cron) ReplaceBatchConsumer(batchConsumer BatchConsumer) (*gocron.Job, error) {
	_ = c.scheduler.RemoveByTag(batchConsumer.TopicId())
	return c.scheduler.Every(batchConsumer.BatchPeriodSec()).Seconds().
		StartAt(time.Now().Add(time.Duration(rand.Intn(batchConsumer.BatchPeriodSec())) * time.Second)).
		Tag(batchConsumer.TopicId()).
		Do(batchConsumer.RunJob)
}

func (c *Cron) RemoveBatchConsumer(batchConsumer BatchConsumer) error {
	return c.scheduler.RemoveByTag(batchConsumer.TopicId())
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
		c.Infof("Cron scheduler stopped")
	case <-time.After(time.Duration(c.config.ShutdownTimeoutSec) * time.Second):
		c.Warnf("Shutdown timeout [%ds] expired. Scheduler will be stopped forcibly. Active batches may be interrupted abruptly", c.config.ShutdownTimeoutSec)
	}
}
