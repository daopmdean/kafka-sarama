package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	saramaConf := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, saramaConf)
	if err != nil {
		log.Fatal("Failed to create consumer:", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("test-topic", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Failed to start partition consumer:", err)
	}
	defer partitionConsumer.Close()

	fmt.Println("Waiting for messages...")

	// Listen for messages with graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Println("-------------------------------")
			fmt.Printf("Received message: %s\n", string(msg.Value))
			if len(msg.Key) > 0 {
				fmt.Printf("Key: %s\n", string(msg.Key))
			}
			fmt.Printf("Partition: %d\n", msg.Partition)
			fmt.Printf("Offset: %d\n", msg.Offset)
			fmt.Printf("Timestamp: %v\n", msg.Timestamp)
		case <-signals:
			break ConsumerLoop
		}
	}

	// Listen for messages without graceful shutdown
	// for msg := range partitionConsumer.Messages() {
	// 	fmt.Println("-------------------------------")
	// 	fmt.Printf("Received message: %s\n", string(msg.Value))
	// 	if len(msg.Key) > 0 {
	// 		fmt.Printf("Key: %s\n", string(msg.Key))
	// 	}
	// 	fmt.Printf("Partition: %d\n", msg.Partition)
	// 	fmt.Printf("Offset: %d\n", msg.Offset)
	// 	fmt.Printf("Timestamp: %v\n", msg.Timestamp)
	// }

}
