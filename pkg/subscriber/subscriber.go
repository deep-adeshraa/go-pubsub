package main

import (
	"fmt"

	mypubsub "github.com/deep-adeshraa/pubsub/pkg/mypubsub"
)

func main() {
	Subscriber := mypubsub.NewSubscriber("50054")

	Subscriber.Subscribe("topic1")

	for {
		fmt.Println("Waiting for messages...")
		select {
		case msg := <-Subscriber.Messages:
			fmt.Println("Received message: ", msg)
		}
	}
}
