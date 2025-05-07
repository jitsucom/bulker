package app

import (
	"github.com/go-co-op/gocron/v2"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
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

func (c *Cron) AddBatchConsumer(destinationId string, batchConsumer BatchConsumer) (gocron.Job, error) {
	options := []gocron.JobOption{gocron.WithTags(batchConsumer.TopicId())}
	if batchConsumer.BatchPeriodSec() > 1 {
		// spread start time to avoid all batch run at the same time
		delay := utils.HashStringInt(destinationId) % uint32(batchConsumer.BatchPeriodSec())
		// introduce small fluctuation to avoid all batches starting at the same time for multi table destinations
		delay += utils.HashStringInt(batchConsumer.TopicId()) % 30
		//don't do small delays. gocron doesn't like StartDateTime at past. with small delays that may be possible
		if delay > 5 {
			options = append(options, gocron.WithStartAt(gocron.WithStartDateTime(time.Now().Add(time.Duration(delay)*time.Second))))
		}
	}
	return c.scheduler.NewJob(gocron.DurationJob(time.Duration(batchConsumer.BatchPeriodSec())*time.Second),
		gocron.NewTask(batchConsumer.RunJob),
		options...)
}

func (c *Cron) ReplaceBatchConsumer(destinationId string, batchConsumer BatchConsumer) (gocron.Job, error) {
	c.RemoveBatchConsumer(batchConsumer)
	return c.AddBatchConsumer(destinationId, batchConsumer)
}

func (c *Cron) RemoveBatchConsumer(batchConsumer BatchConsumer) {
	jobs := c.scheduler.Jobs()
	for _, job := range jobs {
		if utils.ArrayContains(job.Tags(), batchConsumer.TopicId()) {
			err := c.scheduler.RemoveJob(job.ID())
			if err != nil {
				c.Errorf("Error removing job for[%s] id: %s: %v", batchConsumer.TopicId(), job.ID(), err)
			}
		}
	}
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
