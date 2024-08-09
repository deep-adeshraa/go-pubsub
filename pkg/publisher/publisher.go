package main

import (
	"os"

	pubsub "github.com/deep-adeshraa/pubsub/pkg/mypubsub"
)

func main() {
	Publisher, err := pubsub.NewPublisher("50054")

	if err != nil {
		panic(err)
	}

	// get the command line arguments in format of topic and message
	// and publish the message to the topic
	// go run publisher.go topic1 "Hello, World!"
	args := os.Args[1:]

	Publisher.Publish(args[0], []byte(args[1]))
}
