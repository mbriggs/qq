package client

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Message struct {
	Body    string
	Receipt string
}

type QQClient interface {
	DeleteMessage(string, *Message) error
	SendMessage(string, string) error
	SendError(string, string) error
	ReceiveMessage(string) (*Message, error)
	CreateQueue(string) error
}

var _ QQClient = new(sqsClient)
var _ QQClient = new(memoryClient)

func New(queues map[string]string, waitTime int) *sqsClient {
	sesh := session.Must(session.NewSession())
	s := sqs.New(sesh)

	return &sqsClient{queues: queues, waitTime: waitTime, sqs: s}
}

func NewInMemory(queues map[string]string) *memoryClient {
	var c memoryClient
	c.queues = make(map[string][]*Message)

	for k := range queues {
		c.CreateQueue(k)
	}

	return &c
}
