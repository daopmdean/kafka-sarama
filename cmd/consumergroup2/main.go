package main

import (
	"context"

	"github.com/IBM/sarama"
)

type MyConsumer struct {
	ready chan bool
}

func (c *MyConsumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready) // signal that the consumer is ready
	return nil
}

func (c *MyConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *MyConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	return nil
}

func main() {
	conf := sarama.NewConfig()
	conf.Version = sarama.DefaultVersion

	group, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "my-group", conf)
	if err != nil {
		panic(err)
	}

	consumer := MyConsumer{}

	go func() {
		for {
			err := group.Consume(context.Background(), []string{"my-topic"}, &consumer)
			if err != nil {
				panic(err)
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready

}
