package client

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInMemoryClientSendsMessages(t *testing.T) {
	c := memory()
	assert.Nil(t, c.SendMessage("queue", "data"))
	q := c.queue("queue")

	assert.Len(t, q, 1)
	assert.Equal(t, q[0].Body, "data")
}

func TestInMemoryClientSendsErrors(t *testing.T) {
	c := memory()
	assert.Nil(t, c.SendError("queue", "data"))
	q := c.queue("queue--errors")

	assert.Len(t, q, 1)
	assert.Equal(t, q[0].Body, "data")
}

func TestInMemoryClientDeletesMessage(t *testing.T) {
	c := memory()
	assert.Nil(t, c.SendMessage("queue", "data"))
	msg := c.queue("queue")[0]

	assert.Nil(t, c.DeleteMessage("queue", msg))
	assert.Len(t, c.queue("queue"), 0)
}

func TestInMemoryClientErrorsOnMissingMsg(t *testing.T) {
	c := memory()
	msg := Message{Receipt: "foo", Body: "bar"}

	assert.NotNil(t, c.DeleteMessage("queue", &msg))
}

func TestInMemoryClientReceivesMessage(t *testing.T) {
	c := memory()
	assert.Nil(t, c.SendMessage("queue", "data"))

	msg, err := c.ReceiveMessage("queue")

	assert.Nil(t, err)
	assert.Equal(t, "data", msg.Body)
}

func TestInMemoryClientReceivesNothing(t *testing.T) {
	c := memory()

	msg, err := c.ReceiveMessage("queue")

	assert.Nil(t, err)
	assert.Nil(t, msg)
}

func memory() *memoryClient {
	return NewInMemory(map[string]string{"queue": "QUEUE"})
}
