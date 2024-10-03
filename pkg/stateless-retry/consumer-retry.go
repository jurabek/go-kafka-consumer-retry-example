package statelessretry

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ErrKafkaRetrieble struct {
	Err error
}

func (e *ErrKafkaRetrieble) Error() string {
	return fmt.Sprintf("handling kafka message failed: %s", e.Err.Error())
}

type Handler interface {
	Hanle(msg *kafka.Message) error
	MoveToDQL(msg *kafka.Message) error
}

type ConsumerWithRetryOption struct {
	Handler       Handler
	Consumer      *kafka.Consumer
	RetryQueue    chan *kafka.Message // make sure you passed buffered channel to get concurrent retries otherwies new messages will wait unitl previous retries are finished
	MaxRetryCount int
	Backoff       backoff.BackOff
}

func ConsumeWithRetry(ctx context.Context, option *ConsumerWithRetryOption) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case retryMsg := <-option.RetryQueue:
				go func() {
					retries := 1
					for {
						log.Printf("retrying msg: %v times: %v", string(retryMsg.Key), retries)
						if retries >= option.MaxRetryCount {
							option.Handler.MoveToDQL(retryMsg)
							break
						}
						time.Sleep(option.Backoff.NextBackOff())
						retries++
					}
				}()
			}
		}
	}()

	for {
		e := option.Consumer.Poll(100)
		switch m := e.(type) {
		case *kafka.Message:
			if err := option.Handler.Hanle(m); err != nil {
				log.Printf("handling message failed: %v err: %v\n", string(m.Key), err.Error())
				option.RetryQueue <- m
			}
		}
	}
}
