package main

import (
	"EDA/internal"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
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

	if err := client.CreateQueue("customer_created", true, false); err != nil {
		log.Fatal(err)
	}

	if err := client.CreateQueue("customer_test", false, true); err != nil {
		log.Fatal(err)
	}

	if err := client.CreateBinding("customer_created", "customer.created.*", "customers_events"); err != nil {
		log.Fatal(err)
	}

	if err := client.CreateBinding("customer_test", "customer.test.*", "customers_events"); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Send(ctx, "customer_created", "customer.created.us", amqp091.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp091.Persistent,
		Body:         []byte(`A cool mesage between services`),
	}); err != nil {
		log.Fatal(err)
	}

	time.Sleep(10 * time.Second)
	fmt.Println(client)
}
