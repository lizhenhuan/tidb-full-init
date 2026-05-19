package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kafka-consume <topic> [bootstrap]")
		os.Exit(1)
	}
	topic := os.Args[1]
	brokers := "127.0.0.1:9092"
	if len(os.Args) >= 3 {
		brokers = os.Args[2]
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokers},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  1e6,
	})
	defer r.Close()

	// Read from beginning
	r.SetOffset(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}
		fmt.Printf("Key: %s\nValue: %s\n---\n", string(m.Key), string(m.Value))
	}
}
