package utils

import "time"

type Ticker struct {
	ticker *time.Ticker
	C      chan time.Time
	closed chan struct{}
}

func NewTicker(d time.Duration, startDelay time.Duration) *Ticker {
	c := make(chan time.Time, 1)
	closed := make(chan struct{})
	t := &Ticker{
		ticker: time.NewTicker(d),
		C:      c,
		closed: closed,
	}
	t.start(startDelay)
	return t
}

// start
func (t *Ticker) start(startDelay time.Duration) {
	go func() {
		select {
		case tm := <-time.After(startDelay):
			t.C <- tm
		case <-t.closed:
			return
		}
		for {
			select {
			case tm := <-t.ticker.C:
				t.C <- tm
			case <-t.closed:
				return
			}
		}
	}()
}

// Stop turns off a ticker. After Stop, no more ticks will be sent.
func (t *Ticker) Stop() {
	t.ticker.Stop()
	close(t.closed)
}
