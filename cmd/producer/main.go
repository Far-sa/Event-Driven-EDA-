package main

import (
	"context"
	"fmt"
	"log"
	"EDA/internal"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("teo", "password", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// all consuming will be use this connection
	consumeConn, err := internal.ConnectRabbitMQ("teo", "password", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer consumeConn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// consumeClient  ==> direct exchange
	consumeClient, err := internal.NewRabbitMQClient(consumeConn)
	if err != nil {
		panic(err)
	}
	defer consumeClient.Close()

	queue, err := consumeClient.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := consumeClient.CreateBinding(queue.Name, queue.Name, "customer_callbacks"); err != nil {
		panic(err)
	}

	messagBus, err := consumeClient.Consume(queue.Name, "customer_api", true)
	if err != nil {
		panic(err)
	}

	go func() {
		for message := range messagBus {
			log.Printf("Message callbacks %s\n", message.CorrelationId)
		}
	}()

	// create ctx
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	for i := 0; i < 10; i++ {

		if err := client.Send(ctx, "customer_events", "customers.created.us", amqp091.Publishing{
			ContentType:   "text/plain",
			DeliveryMode:  amqp091.Persistent,
			ReplyTo:       queue.Name,
			CorrelationId: fmt.Sprintf("customers_created_%d", i),
			Body:          []byte(`A cool message from services`),
		}); err != nil {
			panic(err)
		}
	}

	log.Println(client)
	var blocking chan struct{}
	<-blocking
}
