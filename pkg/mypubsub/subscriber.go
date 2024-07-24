package mypubsub

import (
	context "context"
	"fmt"
	"crypto/rand"
	"encoding/base64"
	grpc "google.golang.org/grpc"
)

type Subscriber struct {
	Id              string
	topics          []string
	context         context.Context
	cancel          context.CancelFunc
	brokerHost      string
	brokerPort      string
	grpc_connection *grpc.ClientConn
	grpc_client     PubSubServiceClient
	Messages        chan *PubSubMessage
}

func generateRandomString(length int) string {
	randomBytes := make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		fmt.Println("Error generating random string: ", err)
		return ""
	}
	randomString := base64.URLEncoding.EncodeToString(randomBytes)
	return randomString[:length]
}

func NewSubscriber(port string) *Subscriber {
	context, cancel := context.WithCancel(context.Background())

	random_id := "subscriber-" + generateRandomString(10)

	Subscriber := Subscriber{
		Id:         random_id,
		context:    context,
		topics:     []string{},
		cancel:     cancel,
		brokerHost: "localhost",
		brokerPort: port,
		Messages:   make(chan *PubSubMessage),
	}
	url := Subscriber.brokerHost + ":" + Subscriber.brokerPort
	Subscriber.grpc_connection, _ = grpc.Dial(url, grpc.WithInsecure())
	Subscriber.grpc_client = NewPubSubServiceClient(Subscriber.grpc_connection)

	return &Subscriber
}

func (s *Subscriber) Subscribe(topic string) {
	// check if topic is already subscribed
	for _, t := range s.topics {
		if t == topic {
			fmt.Print("Already subscribed to topic: ", topic)
			return
		}
	}

	s.topics = append(s.topics, topic)

	// subscribe to topic
	stream, err := s.grpc_client.Subscribe(s.context, &SubscribeRequest{Topic: topic, SubscriberId: s.Id})

	if err != nil {
		fmt.Println("Error subscribing to topic: ", err)
		return
	}
	fmt.Println("Subscribed to topic: ", topic)

	go s.receiveMessages(stream)
}

func (s *Subscriber) receiveMessages(stream PubSubService_SubscribeClient) {
	for {
		select {
		case <-s.context.Done():
			stream.CloseSend()
			return
		default:
			msg, err := stream.Recv()
			if err != nil {
				fmt.Println("Error receiving message: ", err)
				return
			}
			// Push the message to the channel.
			s.Messages <- msg
		}
	}
}
