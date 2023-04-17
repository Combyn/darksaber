package pulse

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/Combyn/darksaber"
	"github.com/Combyn/darksaber/events"
	"github.com/Combyn/darksaber/platform"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestInit(t *testing.T) {
	addrs := "pulsar://localhost:6650"
	opts := darksaber.Options{
		ServiceName:         "test-service",
		Address:             addrs,
		CertContent:         "",
		AuthenticationToken: "",
	}
	store, err := Init(opts)
	assert.Nil(t, err)

	type S struct {
		Message string `json:"message"`
	}
	go func() {
		time.Sleep(4 * time.Second)
		value := &S{Message: "Hello, World!"}
		e := &events.Event{Data: value, Id: generateRandomName()}
		err = store.PublishRaw("test-topic", e)
	}()

	c := make(chan struct{}, 1)
	go func() {
		err = store.Subscribe("test-topic", func(event platform.Event) {
			value := &S{}
			data, err := event.Parse(value)
			assert.Nil(t, err)
			assert.NotNil(t, data)
			assert.Equal(t, value.Message, "Hello, World!")
			t.Log(event.Topic())
			t.Log(value.Message)
			c <- struct{}{}
			event.Ack()
		})
	}()

	<-c
}

func TestPulsarStore_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	store, _ := InitTestEventStore("nil", logger)

	now := time.Now()
	// cancel after 3secs
	time.AfterFunc(3*time.Second, func() {
		t.Log("cancelling...")
		cancel()
	})
	store.Run(ctx, func() error {
		t.Log("first function")
		return nil
	}, func() error {
		t.Log("second function ")
		return nil
	})
	interval := time.Now().Sub(now)
	if interval.Seconds() < 3 {
		t.Fail()
	}
}

func Test_generateRandomName(t *testing.T) {
	assert.NotEmpty(t, generateRandomName())
}

func TestIntegration(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	bf, err := InitTestEventStore("test-event-store", logger)
	assert.Nil(t, err)
	assert.NotNil(t, bf)

	type e struct {
		Message string `json:"message"`
	}
	value := &e{Message: "Yello"}
	ev := events.New().WithSource("test").WithData(value)
	h := &eventHandler{bf: bf}
	err = bf.PublishRaw("test-topic", ev)

	err = h.handleEvent()
	assert.Nil(t, err)
}

type eventHandler struct {
	bf darksaber.EventStore
}

func (e *eventHandler) handleEvent() error {
	return e.bf.Subscribe("test-topic", func(event platform.Event) {
		log.Println("handling event...")
		type E struct {
			Message string `json:"message"`
		}
		value := &E{}
		d, err := event.Parse(value)
		log.Println(err)
		log.Println(d)
		log.Println(value.Message)
	})
}
