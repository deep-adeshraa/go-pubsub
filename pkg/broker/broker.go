package main

import (
	mypubsub "github.com/deep-adeshraa/pubsub/pkg/mypubsub"
)

func main() {
	Broker := mypubsub.NewBroker("50054")
	Broker.Start()
}
