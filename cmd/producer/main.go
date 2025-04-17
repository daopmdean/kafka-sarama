package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"

	"github.com/IBM/sarama"
)

func main() {
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal("Failed to start Sarama producer:", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: "test-topic",
		Key:   sarama.StringEncoder("Key"),
		Value: sarama.StringEncoder(fmt.Sprintf("Message value %d", rand.Intn(math.MaxInt32))),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatal("Failed to send message:", err)
	}

	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", msg.Topic, partition, offset)
}
