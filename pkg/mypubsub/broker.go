package mypubsub

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
)

type subscriberStream PubSubService_SubscribeServer

type Broker struct {
	UnimplementedPubSubServiceServer                               // embedded unimplemented server so that it satisfies the interface
	topics_subscribers_map           map[string][]string           // subscriber id to topic
	subscriber_streams_map           map[string][]subscriberStream // subscriber id to stream
	context                          context.Context
	cancel                           context.CancelFunc
	port                             string
	listener                         net.Listener
	grpcServer                       *grpc.Server
	host                             string
}

func NewBroker(port string) *Broker {
	context, cancel := context.WithCancel(context.Background())
	return &Broker{
		topics_subscribers_map: make(map[string][]string),
		subscriber_streams_map: make(map[string][]subscriberStream),
		context:                context,
		cancel:                 cancel,
		port:                   port,
		host:                   "localhost",
	}
}

func (b *Broker) createGrpcServer() {
	listener, err := net.Listen("tcp", b.host+":"+b.port)

	if err != nil {
		fmt.Println("Failed to listen: ", err)
		return
	}

	grpcServer := grpc.NewServer()
	b.listener = listener
	b.grpcServer = grpcServer
	RegisterPubSubServiceServer(grpcServer, b)

	if err := b.grpcServer.Serve(b.listener); err != nil {
		zap.S().Fatalf("Failed to serve: %v", err)
	}
}

func (b *Broker) Start() {
	b.createGrpcServer()
}

func (b *Broker) Publish(ctx context.Context, in *PublishRequest) (*PublishResponse, error) {
	fmt.Println("Received publish request for topic: ", in.GetTopic())

	subscribers := b.topics_subscribers_map[in.GetTopic()]
	// topics_subscribers_map           map[string][]string           // subscriber id to topic
	// subscriber_streams_map           map[string][]subscriberStream // subscriber id to stream

	for _, subscriber := range subscribers {
		stream := b.subscriber_streams_map[subscriber]
		fmt.Println("Sending message to subscriber: ", subscriber, " for topic: ", in.GetTopic())

		for _, s := range stream {
			err := s.Send(&PubSubMessage{Topic: in.GetTopic(), Data: in.GetData()})
			if err != nil {
				fmt.Println("Error sending message to subscriber: ", err)
			}
		}
	}

	return &PublishResponse{
		Topic: in.GetTopic(),
		Data:  in.GetData(),
	}, nil
}

func (b *Broker) Subscribe(in *SubscribeRequest, stream PubSubService_SubscribeServer) error {
	fmt.Println("Received subscriber: ", in.GetSubscriberId(), " for topic: ", in.GetTopic())

	b.subscriber_streams_map[in.GetSubscriberId()] = append(b.subscriber_streams_map[in.GetSubscriberId()], stream)
	b.topics_subscribers_map[in.GetTopic()] = append(b.topics_subscribers_map[in.GetTopic()], in.GetSubscriberId())

	for {
		select {
		// Wait for the client to close the stream
		case <-stream.Context().Done():
			return nil
			// Wait for the broker to shutdown
		case <-b.context.Done():
			return nil
		}
	}
}

func (b *Broker) Unsubscribe(ctx context.Context, in *UnsubscribeRequest) (*Empty, error) {
	fmt.Println("Unsubscribing subscriber: ", in.GetSubscriberId(), " from topic: ", in.GetTopic())

	delete(b.topics_subscribers_map, in.GetTopic())
	delete(b.subscriber_streams_map, in.GetSubscriberId())

	return &Empty{}, nil
}
