package client

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strings"
)

type sqsClient struct {
	sqs      *sqs.SQS
	queues   map[string]string
	waitTime int
}

func (c *sqsClient) DeleteMessage(queue string, msg *Message) error {
	_, err := c.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		ReceiptHandle: aws.String(msg.Receipt),
		QueueUrl:      c.QueueURL(queue),
	})

	if err != nil {
		return fmt.Errorf("could not delete message: %v", err)
	}

	return nil
}

func (c *sqsClient) SendMessage(queue string, data string) error {
	_, err := c.sqs.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(data),
		QueueUrl:    c.QueueURL(queue),
	})

	if err != nil {
		return fmt.Errorf("could not send message to %s: %v", queue, err)
	}

	return nil
}

func (c *sqsClient) SendError(queue string, data string) error {
	_, err := c.sqs.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(data),
		QueueUrl:    c.ErrorQueueURL(queue),
	})

	if err != nil {
		return fmt.Errorf("could not send error to %s: %v", queue, err)
	}

	return nil
}

func (c *sqsClient) ReceiveMessage(queue string) (*Message, error) {
	out, err := c.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            c.QueueURL(queue),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(int64(c.waitTime)),
	})

	if err != nil {
		return nil, fmt.Errorf("could not receive message: %v", err)
	}

	count := len(out.Messages)
	var result *sqs.Message

	switch {
	case count == 0:
		result = nil
	case count == 1:
		result = out.Messages[0]
	case true:
		return nil, fmt.Errorf("unexpected messages count - %d", count)
	}

	return &Message{Body: *result.Body, Receipt: *result.ReceiptHandle}, nil
}

func (c *sqsClient) CreateQueue(queueURL string) error {
	tokens := strings.Split(queueURL, "/")
	_, err := c.sqs.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(tokens[len(tokens)-1]),
	})

	if err != nil {
		return fmt.Errorf("could not create queue %s: %v", queueURL, err)
	}

	return nil
}

func (c *sqsClient) QueueURL(name string) *string {
	return aws.String(c.queues[name])
}

func (c *sqsClient) ErrorQueueURL(name string) *string {
	return aws.String(fmt.Sprintf("%s--errors", c.queues[name]))
}
