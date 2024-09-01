package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
	"github.com/joho/godotenv"
	"github.com/kenta-ja8/pubsub-barrier-sync/pkg/adapter"
)

func execute() {
	log.Println("Hello World")
	defer log.Println("Goodbye World")

	projectID := os.Getenv("PROJECT_ID")
	topicID := os.Getenv("TOPIC_ID")
	subscriptionID := os.Getenv("SUBSCRIPTION_ID")

	ctx, cancel := context.WithCancel(context.Background())
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err, "Failed to create client")
	}
	defer client.Close()

	topic := client.Topic(topicID)

	go func() {
		log.Println("Receiving message")
		sub := client.Subscription(subscriptionID)
		sub.ReceiveSettings.MaxOutstandingMessages = 4
		err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			log.Printf("Got message: %s %q\n", msg.ID, string(msg.Data))
			defer log.Printf("Done: %s %q\n", msg.ID, string(msg.Data))
			err := adapter.DispatchMessage(ctx, msg, topic)
			if err != nil {
				log.Println("Failed to dispatch message", err)
			}
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	err = adapter.StartPublish(ctx, topic)
	if err != nil {
		log.Fatal(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("Received Ctrl+C, shutting down...")
	cancel()

	log.Println("Done")
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	execute()
}
