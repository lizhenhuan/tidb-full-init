package kafka

import "context"

type Message struct {
	Key   []byte
	Value []byte
	Topic string
}

type Producer interface {
	Send(ctx context.Context, msg *Message) error
	Close() error
}
