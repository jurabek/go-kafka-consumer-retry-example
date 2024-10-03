package statefullretry

import (
	"log"
	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Max number of failures before rejecting a message.
const DEFAULT_MAX_FAILURES = 10

// SeekUtils contains utility functions for handling Kafka seeks.
type SeekUtils struct{}

// Seek records to the earliest position, optionally skipping the first record.
func (su SeekUtils) DoSeeks(records []*kafka.Message, consumer *kafka.Consumer, exception error, recoverable bool, skipper func(*kafka.Message, error) bool) bool {
	partitions := make(map[kafka.TopicPartition]kafka.Offset)
	var first int32 = 1 // Atomic boolean in Go to indicate the first record
	var skipped int32 = 0

	for _, record := range records {
		// Handle skipping the first record
		if recoverable && atomic.LoadInt32(&first) == 1 {
			test := skipper(record, exception)
			if test {
				log.Printf("Skipping seek of: %s", record.TopicPartition)
				atomic.StoreInt32(&skipped, 1)
			} else {
				atomic.StoreInt32(&skipped, 0)
			}
		}

		// If not recoverable or not the first record or didn't skip, we seek
		if !recoverable || atomic.LoadInt32(&first) == 0 || atomic.LoadInt32(&skipped) == 0 {
			tp := kafka.TopicPartition{Topic: record.TopicPartition.Topic, Partition: record.TopicPartition.Partition}
			partitions[tp] = kafka.Offset(record.TopicPartition.Offset)
		}

		// Set first to false after the first record
		atomic.StoreInt32(&first, 0)
	}

	// Perform seek operation for the relevant partitions
	su.SeekPartitions(consumer, partitions)
	return atomic.LoadInt32(&skipped) == 1
}

// Seek records on each partition.
func (su SeekUtils) SeekPartitions(consumer *kafka.Consumer, partitions map[kafka.TopicPartition]kafka.Offset) {
	for tp, offset := range partitions {
		log.Printf("Seeking %s to offset: %d", tp, offset)
		err := consumer.Seek(tp, 0)
		if err != nil {
			log.Printf("Failed to seek partition %s to offset %d: %s", tp, offset, err)
		}
	}
}

// Seek or recover from the exception by seeking the records.
func (su SeekUtils) SeekOrRecover(thrownException error, records []*kafka.Message, consumer *kafka.Consumer, recoverable bool, skipper func(*kafka.Message, error) bool, commitRecovered bool) {
	if len(records) == 0 {
		if thrownException != nil {
			log.Fatalf("Error: No records to process, but exception encountered: %s", thrownException)
		} else {
			log.Fatalf("Error: No records to process, and no exception provided")
		}
	}

	// Seek the records
	if !su.DoSeeks(records, consumer, thrownException, true, skipper) {
		log.Fatalf("Failed to recover or seek to current after exception: %s", thrownException)
	}

	// Optionally commit the recovered record offset
	if commitRecovered {
		record := records[0]
		tp := kafka.TopicPartition{Topic: record.TopicPartition.Topic, Partition: record.TopicPartition.Partition, Offset: kafka.Offset(record.TopicPartition.Offset + 1)}
		offsets := []kafka.TopicPartition{tp}
		_, err := consumer.CommitOffsets(offsets)
		if err != nil {
			log.Printf("Failed to commit recovered record offset: %s", err)
		} else {
			log.Printf("Committed recovered record offset for partition %s", record.TopicPartition)
		}
	}
}
