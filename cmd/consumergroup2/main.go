package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

type MyConsumer struct {
	ready chan bool
}

func (c *MyConsumer) Setup(sarama.ConsumerGroupSession) error {

	fmt.Println("seting up my consumer")
	close(c.ready) // signal that the consumer is ready
	return nil
}

func (c *MyConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("cleaning up my consumer")
	return nil
}

func (c *MyConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				fmt.Println("Claim messages channel closed")
				return nil
			}
			fmt.Println("Consume message: ", string(msg.Value), msg.Timestamp)
		case <-session.Context().Done():
			fmt.Println("Session context done")
			return nil
		}
	}
}

func main() {
	conf := sarama.NewConfig()
	conf.Version = sarama.DefaultVersion

	group, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "my-group", conf)
	if err != nil {
		panic(err)
	}

	consumer := MyConsumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			err := group.Consume(ctx, []string{"my-topic"}, &consumer)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					fmt.Println("Consumer group closed")
					return
				}
				panic(err)
			}
			consumer.ready = make(chan bool)
		}
	}()

	fmt.Println("Waiting for consumer to be ready...")
	<-consumer.ready
	fmt.Println("Consumer is ready")

	keep, pause := true, false

	// Wait for a signal to exit
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt)

	siguser := make(chan os.Signal, 1)
	signal.Notify(siguser, syscall.SIGUSR1)
	fmt.Println("Signal to pause/resume consumption: SIGUSR1", os.Getpid())

	for keep {
		select {
		case <-sigterm:
			fmt.Println("terminating: Received interrupt signal")
			keep = false
		case <-siguser:
			if pause {
				pause = false
				group.ResumeAll()
				fmt.Println("Resuming consumption")
			} else {
				pause = true
				group.PauseAll()
				fmt.Println("Pausing consumption")
			}

		case <-ctx.Done():
			fmt.Println("terminating: Context done")
			keep = false
		}
	}

	fmt.Println("Exiting...")
}
