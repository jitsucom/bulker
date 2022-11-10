package app

import (
	"github.com/go-co-op/gocron"
	"math/rand"
	"time"
)

type Cron struct {
	config    *AppConfig
	scheduler *gocron.Scheduler
}

func NewCron(config *AppConfig) *Cron {
	s := gocron.NewScheduler(time.UTC)
	s.TagsUnique()
	s.StartAsync()
	return &Cron{scheduler: s, config: config}
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
	go func() {
		c.scheduler.Stop()
		time.Sleep(5 * time.Second)
	}()
}
