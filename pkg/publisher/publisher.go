package main

import (
	pubsub "github.com/deep-adeshraa/pubsub/pkg/mypubsub"
)

func main() {
	Publisher, err := pubsub.NewPublisher("50054")

	if err != nil {
		panic(err)
	}

	Publisher.Publish("topic1", []byte("Hello World!"))
}
