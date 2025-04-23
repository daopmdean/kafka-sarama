package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type Consumer struct{}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer group setup")
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer group cleanup")
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Process messages
	for msg := range claim.Messages() {
		log.Printf("Consumed message offset=%d, partition=%d, key=%s, value=%s",
			msg.Offset, msg.Partition, string(msg.Key), string(msg.Value))

		// ✅ Mark message as processed — this commits the offset
		// session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	brokers := []string{"localhost:9092"}
	groupID := "my-consumer-group-2"
	topics := []string{"my-topic", "test-topic"}

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0                 // Match your Kafka version
	config.Consumer.Offsets.AutoCommit.Enable = true // ✅ Manual commit
	config.Consumer.Offsets.AutoCommit.Interval = time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer := &Consumer{}

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer group.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
		<-sigterm
		cancel()
	}()

	for i, v := range topics {
		log.Printf("Topic %d: %s\n", i, v)
	}

	log.Println("Starting consumer group...")
	for {
		if err := group.Consume(ctx, topics, consumer); err != nil {
			log.Printf("Error during consumption: %v", err)
		}
		if ctx.Err() != nil {
			break
		}
	}

	log.Println("Consumer stopped")
}
