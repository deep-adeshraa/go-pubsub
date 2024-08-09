package main

import (
	"fmt"
	"os"

	mypubsub "github.com/deep-adeshraa/pubsub/pkg/mypubsub"
)

func main() {
	Subscriber := mypubsub.NewSubscriber("50054")
	args := os.Args[1:]

	Subscriber.Subscribe(args[0])

	for {
		fmt.Println("Waiting for messages...")
		select {
		case msg := <-Subscriber.Messages:
			fmt.Println("Received message: ", msg)
		}
		
	}
}
