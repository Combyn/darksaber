package darksaber

type Event interface {
	Ack()
	Data() []byte
	Topic() string
}
