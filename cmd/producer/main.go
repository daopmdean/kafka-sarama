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
		Topic: "my-topic",
		Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", rand.Intn(2))),
		Headers: []sarama.RecordHeader{
			{Key: []byte("header-key"), Value: []byte("header-value")},
			{Key: []byte("header-key"), Value: []byte("header-value-duplicate")},
			{Key: []byte("header-key-2"), Value: []byte("header-value")},
		},
		Value: sarama.StringEncoder(fmt.Sprintf(`{"orderId":123,"status":"created","message":"value %d"}`, rand.Intn(math.MaxInt32))),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatal("Failed to send message:", err)
	}

	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", msg.Topic, partition, offset)
}
