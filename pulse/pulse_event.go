package pulse

import (
	"github.com/Combyn/darksaber"
	"github.com/apache/pulsar-client-go/pulsar"
)

type event struct {
	raw      pulsar.Message
	consumer pulsar.Consumer
}

func NewEvent(message pulsar.Message, consumer pulsar.Consumer) darksaber.Event {
	return &event{raw: message, consumer: consumer}
}

func (e *event) Data() []byte {
	return e.raw.Payload()
}

func (e *event) Topic() string {
	t := e.raw.Topic()
	// Manually agree what we want the topic to look like from pulsar.
	return t
}

func (e *event) Ack() {
	e.consumer.AckID(e.raw.ID())
}
