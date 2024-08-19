package main

import (
	"context"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

var projectID = "your-project-id"
var pubsubID = "your-subscription-id"
var topicID = "your-topic-id"

func main() {
	log.Println("Hello World")
	defer log.Println("Goodbye World")
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	client.Subscription(pubsubID).Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		log.Printf("Got message: %s %q\n", msg.ID, string(msg.Data))
	})

	topic := client.Topic(topicID)
	res := topic.Publish(ctx, &pubsub.Message{
		Data: []byte("DataXXX"),
	})
	println(res)

	time.Sleep(10 * time.Second)

}
