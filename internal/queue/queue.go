package queue

import (
	"context"
	"simtek-worker/internal/config"

	"github.com/rabbitmq/amqp091-go"
)

type QueueReader interface {
	Read(chan<- string, context.Context) error
}

type RabbitQueueReader struct {
	queueConfig config.QueueConfig
}

func NewRabbitQueueReader() QueueReader {
	queueReader := &RabbitQueueReader{}

	return queueReader
}

func (qr *RabbitQueueReader) Read(outputChan chan<- string, ctx context.Context) error {
	conn, err := amqp091.Dial(qr.queueConfig.ConnectionString)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return nil
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare(
		qr.queueConfig.QName,
		false,
		false,
		false,
		false,
		nil,
	)

	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)

	for {
		select {
		case msg := <-msgs:
			outputChan <- string(msg.Body)
		case <-ctx.Done():
			return ctx.Err() // Return the context error
		}
	}
}
