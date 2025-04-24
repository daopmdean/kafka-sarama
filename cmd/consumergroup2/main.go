package main

import (
	"context"

	"github.com/IBM/sarama"
)

type MyConsumer struct {
	ready chan bool
}

func (c *MyConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *MyConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *MyConsumer) ConsumeClaim(sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) error {
	return nil
}

func main() {
	conf := sarama.NewConfig()
	conf.Version = sarama.DefaultVersion

	group, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "my-group", conf)
	if err != nil {
		panic(err)
	}

	err = group.Consume(context.Background(), []string{"my-topic"}, &MyConsumer{})
	if err != nil {
		panic(err)
	}
}
