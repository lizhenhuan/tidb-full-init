package kafka

import (
	"context"
	"fmt"
	"log"
)

type ConsoleProducer struct {
	messages []*Message
}

func NewConsoleProducer() *ConsoleProducer {
	return &ConsoleProducer{messages: nil}
}

func (p *ConsoleProducer) Send(_ context.Context, msg *Message) error {
	log.Printf("[KAFKA] topic=%s key=%s value=%s",
		msg.Topic,
		string(msg.Key),
		string(msg.Value),
	)
	p.messages = append(p.messages, msg)
	return nil
}

func (p *ConsoleProducer) Close() error {
	log.Printf("ConsoleProducer closed, total messages: %d", len(p.messages))
	return nil
}

func (p *ConsoleProducer) MessageCount() int {
	return len(p.messages)
}

var _ = fmt.Sprintf
