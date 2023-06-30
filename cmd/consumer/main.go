package main

import (
	"context"
	"log"
	"EDA/internal"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("teo", "password", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	publishConn, err := internal.ConnectRabbitMQ("teo", "password", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer publishConn.Close()

	// client
	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	publishClient, err := internal.NewRabbitMQClient(publishConn)
	if err != nil {
		panic(err)
	}
	defer publishClient.Close()

	// methods
	queue, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := client.CreateBinding(queue.Name, "", "customer_events"); err != nil {
		panic(err)
	}

	messageBus, err := client.Consume(queue.Name, "email_service", false)
	if err != nil {
		panic(err)
	}

	// Set a timeout for 15 seconds
	ctx := context.Background()

	blocking := make(chan string)

	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// apply limit on RB server
	if err := client.ApplyQos(10, 0, true); err != nil {
		panic(err)
	}

	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			// spawn a worker
			msg := message
			g.Go(func() error {
				log.Printf("New Messages : %v", msg)
				time.Sleep(time.Second * 10)

				if err := msg.Ack(false); err != nil {
					log.Println("failed to ack message")
					return err
				}
				if err := publishClient.Send(ctx, "customer_callbacks", msg.ReplyTo, amqp091.Publishing{
					ContentType:   "text/plain",
					DeliveryMode:  amqp091.Persistent,
					Body:          []byte("RPC Complete"),
					CorrelationId: msg.CorrelationId,
				}); err != nil {
					panic(err)
				}
				log.Printf("Acknowledge message %s\n", msg.MessageId)
				return nil
			})
		}
	}()

	log.Println("Consuming, to close the program press CTRL + C")
	<-blocking

	// blocking := make(chan string)
	//
	// go func() {
	// 	for message := range messageBus {
	// 		log.Printf("New Message : %v", message)
	//
	// 		if !message.Redelivered {
	// 			message.Nack(false, true)
	// 			continue
	// 		}
	// 		if err := message.Ack(false); err != nil {
	// 			log.Println("failed to ack message")
	// 			continue
	// 		}
	// 		log.Printf("Acknowledge message %s\n", message.MessageId)
	// 	}
	// }()
	//
	// log.Println("Consuming, to close the program press CTRL + C")
	//
	// <-blocking
}
