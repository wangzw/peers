package broker

import (
	"log/slog"
	"time"
)

type listenRetrier struct {
	bind        func(addr string) error
	currentAddr func() (string, error)
	clock       Clock
	log         *slog.Logger
	onSuccess   func(addr string) (string, error)
	onFailure   func(addr, bound string, err error)

	wake   chan struct{}
	stopCh chan struct{}
	done   chan struct{}
}

const (
	listenRetryFirst = time.Second
	listenRetryMax   = time.Minute
)

func newListenRetrier(bind func(string) error, currentAddr func() (string, error), clock Clock, log *slog.Logger,
	onSuccess func(string) (string, error), onFailure func(addr, bound string, err error)) *listenRetrier {
	if clock == nil {
		clock = SystemClock()
	}
	return &listenRetrier{
		bind:        bind,
		currentAddr: currentAddr,
		clock:       clock,
		log:         log,
		onSuccess:   onSuccess,
		onFailure:   onFailure,
		wake:        make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
	}
}

func (r *listenRetrier) start() {
	go r.run()
}

func (r *listenRetrier) retryNow() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *listenRetrier) stop() {
	if r == nil {
		return
	}
	close(r.stopCh)
	<-r.done
}

func (r *listenRetrier) run() {
	defer close(r.done)
	delay := listenRetryFirst
	lastReason := ""
	bound := ""
	for {
		fired, stopped := r.wait(delay)
		if stopped {
			return
		}
		addr, err := r.currentAddr()
		if err == nil {
			err = r.bind(addr)
		}
		if err == nil {
			bound = addr
			if r.onSuccess != nil {
				next, successErr := r.onSuccess(addr)
				if successErr != nil {
					err = successErr
					if next != "" {
						addr = next
					}
				} else {
					if next != "" {
						addr = next
					}
					bound = addr
				}
			}
		}
		if err == nil {
			return
		}
		reason := addr + ": " + err.Error()
		if reason != lastReason {
			lastReason = reason
			if r.onFailure != nil {
				r.onFailure(addr, bound, err)
			}
		}
		if fired {
			delay *= 2
			if delay > listenRetryMax {
				delay = listenRetryMax
			}
		} else {
			delay = listenRetryFirst
		}
	}
}

func (r *listenRetrier) wait(d time.Duration) (fired, stopped bool) {
	ch := make(chan struct{}, 1)
	t := r.clock.AfterFunc(d, func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	})
	select {
	case <-r.stopCh:
		t.Stop()
		return false, true
	case <-r.wake:
		t.Stop()
		return false, false
	case <-ch:
		return true, false
	}
}
