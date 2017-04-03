package client

import (
	"fmt"
	"github.com/satori/go.uuid"
)

type memoryClient struct {
	queues map[string][]*Message
}

func (c *memoryClient) DeleteMessage(queue string, msg *Message) error {
	q := c.queue(queue)

	for i, m := range q {
		if m.Receipt == msg.Receipt {
			c.queues[queue] = append(q[:i], q[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("could not find a message with receipt %s", msg.Receipt)
}

func (c *memoryClient) SendMessage(queue string, data string) error {
	q := c.queue(queue)
	c.queues[queue] = append(q, &Message{Receipt: nextReceipt(), Body: data})

	return nil
}

func (c *memoryClient) SendError(queue string, data string) error {
	errorName := fmt.Sprintf("%s--errors", queue)
	q := c.queue(errorName)

	c.queues[errorName] = append(q, &Message{Receipt: nextReceipt(), Body: data})

	return nil
}

func (c *memoryClient) ReceiveMessage(queue string) (*Message, error) {
	q := c.queue(queue)

	var result *Message

	if len(q) == 0 {
		result = nil
	} else {
		result = q[0]
		c.queues[queue] = q[1:]
	}

	return result, nil
}

func (c *memoryClient) CreateQueue(queue string) error {
	var q []*Message
	c.queues[queue] = q

	return nil
}

func (c *memoryClient) queue(name string) (result []*Message) {
	q := c.queues[name]

	if q == nil {
		c.CreateQueue(name)
		result = c.queues[name]
	} else {
		result = q
	}

	return
}

func nextReceipt() string {
	return uuid.NewV4().String()
}
