package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
	"github.com/joho/godotenv"
	"github.com/kenta-ja8/pubsub-barrier-sync/pkg/adapter"
)

func execute() {
	log.Println("Starting execution")
	defer log.Println("Execution finished")

	projectID := os.Getenv("PROJECT_ID")
	topicID := os.Getenv("TOPIC_ID")
	subscriptionID := os.Getenv("SUBSCRIPTION_ID")

	ctx, cancel := context.WithCancel(context.Background())
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	topic := client.Topic(topicID)

	go func() {
		log.Println("Receiving messages")
		sub := client.Subscription(subscriptionID)
		sub.ReceiveSettings.MaxOutstandingMessages = 4
		err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			log.Printf("Received message: ID=%s Data=%q", msg.ID, string(msg.Data))
			defer log.Printf("Processed message: ID=%s Data=%q", msg.ID, string(msg.Data))
			err := adapter.DispatchMessage(ctx, msg, topic)
			if err != nil {
				log.Printf("Failed to dispatch message: %v", err)
			}
		})
		if err != nil {
			log.Fatalf("Failed to receive messages: %v", err)
		}
	}()

	err = adapter.StartPublish(ctx, topic)
	if err != nil {
		log.Fatalf("Failed to start publishing: %v", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("Received termination signal, shutting down...")
	cancel()

	log.Println("Shutdown complete")
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	execute()
}
