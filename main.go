package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	ctx := context.Background()
	kafkaContainer := StartKafkaServer(ctx)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	topic := os.Getenv("KAFKA_TOPIC")
	// kafkaServers := os.Getenv("KAFKA_SERVER")

	// defer func() {
	// 	if err := tp.Shutdown(context.Background()); err != nil {
	// 		log.Printf("Error shutting down tracer provider: %v", err)
	// 	}
	// }()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		log.Fatal(err)
	}

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
	fmt.Println("Subscribed to myTopic")

	// consume messages
	run := true
	for run == true {
		select {
		case sig := <-signals:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:

				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))

			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Println("Closing consumer")
	consumer.Close()
}
