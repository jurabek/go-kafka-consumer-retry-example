package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	bootstrapServers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	deliveryChan := make(chan kafka.Event)
	go func() {
		for i := 0; i < 10; i++ {
			message := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(fmt.Sprintf("message #%v", i)),
				Key:            []byte(fmt.Sprintf("message #%v", i)),
				Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
			}

			err = p.Produce(message, deliveryChan)
			if err != nil {
				log.Printf("Produce failed: %v\n", err)
				os.Exit(1)
			}

			e := <-deliveryChan
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				log.Printf("Delivered message to topic %s [%d] at offset %v\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}
		}
	}()

	sig := <-signals

	fmt.Printf("get signal %v:", sig)
	close(deliveryChan)
}
