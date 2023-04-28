package main

import (
	"EDA/internal"
	"context"
	"log"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("puppet", "admin", "localhost:5672", "customers")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	messageBus, err := client.Consume("customer_created", "email-service", false)
	if err != nil {
		log.Fatal(err)
	}

	var blocking chan struct{}

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			msg := message
			g.Go(func() error {
				log.Printf("New message: %v", msg)
				time.Sleep(10 * time.Second)
				if err := msg.Ack(false); err != nil {
					log.Println("Ack message failed")
					return err
				}
				log.Printf("Acknowledge message %s\n", message.MessageId)
				return nil
			})
			log.Printf("Acknowledge message %s\n", message.MessageId)
		}
	}()
	log.Println("Consuming,to close the app press CTRL+C")

	<-blocking
}
