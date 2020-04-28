package consumer

type Message struct {
	Topic string
	Key   []byte
	Value []byte
}
