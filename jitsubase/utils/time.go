package utils

import "time"

type Ticker struct {
	ticker *time.Ticker
	d      time.Duration
	C      chan time.Time
	Closed chan struct{}
}

func NewTicker(d time.Duration, startDelay time.Duration) *Ticker {
	c := make(chan time.Time, 1)
	closed := make(chan struct{})
	t := &Ticker{
		d:      d,
		C:      c,
		Closed: closed,
	}
	t.start(startDelay)
	return t
}

// start
func (t *Ticker) start(startDelay time.Duration) {
	go func() {
		if startDelay != t.d {
			select {
			case tm := <-time.After(startDelay):
				t.C <- tm
			case <-t.Closed:
				return
			}
		}
		t.ticker = time.NewTicker(t.d)
		for {
			select {
			case tm := <-t.ticker.C:
				t.C <- tm
			case <-t.Closed:
				return
			}
		}
	}()
}

func (t *Ticker) Period() time.Duration {
	return t.d
}

// Stop turns off a ticker. After Stop, no more ticks will be sent.
func (t *Ticker) Stop() {
	t.ticker.Stop()
	close(t.Closed)
}
