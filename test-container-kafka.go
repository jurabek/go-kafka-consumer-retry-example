package main

import (
	"context"
	"log"

	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

func StartKafkaServer(ctx context.Context) *kafka.KafkaContainer {
	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		log.Fatal(err)
	}
	return kafkaContainer
}
