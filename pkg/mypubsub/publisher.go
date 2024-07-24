package mypubsub

import (
	context "context"

	grpc "google.golang.org/grpc"
)

type Publisher struct {
	brokerHost      string
	brokerPort      string
	topics          []string
	grpc_connection *grpc.ClientConn
	context         context.Context
	grpc_client     PubSubServiceClient
	cancel          context.CancelFunc
}

func NewPublisher(port string) (*Publisher, error) {
	context, cancel := context.WithCancel(context.Background())

	NewPublisher := Publisher{
		brokerHost: "localhost",
		brokerPort: port,
		topics:     []string{},
		context:    context,
		cancel:     cancel,
	}

	url := NewPublisher.brokerHost + ":" + NewPublisher.brokerPort

	NewPublisher.grpc_connection, _ = grpc.Dial(url, grpc.WithInsecure())

	NewPublisher.grpc_client = NewPubSubServiceClient(NewPublisher.grpc_connection)

	return &NewPublisher, nil
}

func (p *Publisher) Publish(topic string, message []byte) error {
	_, err := p.grpc_client.Publish(p.context, &PublishRequest{Topic: topic, Data: message})
	if err != nil {
		return err
	}
	return nil
}

func (p *Publisher) Close() {
	defer p.cancel()
	p.grpc_connection.Close()
}
