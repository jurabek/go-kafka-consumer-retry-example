package statelessretry

import (
	"context"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Handler interface {
	Hanle(msg *kafka.Message) error
	MoveToDQL(msg *kafka.Message) error
}

type ConsumerWithRetryOption struct {
	Handler       Handler
	Consumer      *kafka.Consumer
	RetryQueue    chan *kafka.Message
	MaxRetryCount int
}

func ConsumeWithRetry(ctx context.Context, option *ConsumerWithRetryOption) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case retryMsg := <-option.RetryQueue:
				for i := 0; i <= option.MaxRetryCount; i++ {
					log.Printf("retrying msg: %v times: %v", retryMsg, i)
				}
			}
		}
	}()

	for {
		e := option.Consumer.Poll(100)
		switch m := e.(type) {
		case *kafka.Message:
			log.Printf("recieved message: %v", m)
			if err := option.Handler.Hanle(m); err != nil {
				option.RetryQueue <- m
			}
		}
	}
}
