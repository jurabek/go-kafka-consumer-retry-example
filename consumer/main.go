package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	statelessretry "github.com/jurabek/go-kafka-consumer-retry-example/pkg/stateless-retry"
)

type ExternalClientHandler struct{}

// Hanle implements statelessretry.Handler.
func (e *ExternalClientHandler) Hanle(msg *kafka.Message) error {
	fakeBakendUrl := os.Getenv("FAKE_BACKEND_URL")
	url := fmt.Sprintf("http://%s/send-me-request", fakeBakendUrl)

	log.Printf("handling: %v\n", string(msg.Value))

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return &statelessretry.ErrKafkaRetrieble{
			Err: fmt.Errorf("external server returned status code: %v", res.StatusCode),
		}
	}
	return nil
}

// MoveToDQL implements statelessretry.Handler.
func (e *ExternalClientHandler) MoveToDQL(msg *kafka.Message) error {
	log.Printf("moving msg_key: %v into DLQ", string(msg.Key))
	return nil
}

var _ statelessretry.Handler = (*ExternalClientHandler)(nil)

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	topic := os.Getenv("KAFKA_TOPIC")
	brokers := os.Getenv("KAFKA_BROKERS")

	fmt.Printf("broker url: %v\n", brokers)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	// subscribe to the topic
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to topic: %s\n", err)
		os.Exit(1)
	}
	fmt.Println("Subscribed to " + topic)

	ctx, cancel := context.WithCancel(context.Background())
	go statelessretry.ConsumeWithRetry(ctx, &statelessretry.ConsumerWithRetryOption{
		Consumer:      consumer,
		Handler:       &ExternalClientHandler{},
		RetryQueue:    make(chan *kafka.Message, 1000),
		MaxRetryCount: 3,
		Backoff:       backoff.NewExponentialBackOff(backoff.WithInitialInterval(3 * time.Second)),
	})

	sig := <-signals

	fmt.Printf("get signal %v:", sig)
	cancel()

	fmt.Println("Closing consumer")
	consumer.Close()
}
