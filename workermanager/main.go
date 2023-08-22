package workermanager

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type TaskFunc func(item interface{}) (interface{}, error)

type options struct {
	useRateLimit bool
	rateLimit    time.Duration
}

type Option func(*options)

func WithRateLimit(limit time.Duration) Option {
	return func(opts *options) {
		opts.useRateLimit = true
		opts.rateLimit = limit
	}
}

func StartWorkers(concurrentLimit, queueLimit int, taskFunc TaskFunc, input <-chan interface{}, output chan<- interface{}, opts ...Option) {
	defaultOptions := options{} // Default options
	for _, opt := range opts {
		opt(&defaultOptions)
	}

	var limiter <-chan time.Time
	if defaultOptions.useRateLimit {
		ticker := time.NewTicker(defaultOptions.rateLimit)
		defer ticker.Stop()
		limiter = ticker.C
	}

	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 1; i <= concurrentLimit; i++ {
		wg.Add(1)
		go worker(i, input, limiter, taskFunc, output, &wg)
	}

	// Close the output channel when all workers are done
	go func() {
		wg.Wait()
		close(output)
	}()

	fmt.Println("Workers started")
}

func worker(id int, input <-chan interface{}, limiter <-chan time.Time, taskFunc TaskFunc, output chan<- interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for item := range input {
		if limiter != nil {
			<-limiter // Wait for a token from the rate limiter
		}

		result, err := taskFunc(item)
		if err != nil {
			log.Printf("Error processing item %v: %v", item, err)
			continue
		}

		output <- result
	}
}
