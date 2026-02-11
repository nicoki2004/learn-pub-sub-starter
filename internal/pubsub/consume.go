package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}

	// New Table for QueueDeclare Arguments
	args := make(map[string]interface{})
	args["x-dead-letter-exchange"] = "peril_dlx"

	queue, err := ch.QueueDeclare(
		queueName,                       // name
		queueType == SimpleQueueDurable, // durable
		queueType != SimpleQueueDurable, // delete when unused
		queueType != SimpleQueueDurable, // exclusive
		false,                           // no-wait
		args,                            // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	err = ch.QueueBind(
		queue.Name, // queue name
		key,        // routing key
		exchange,   // exchange
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}
	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer (vacío para auto-generación)
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return err
	}

	go func() {
		defer ch.Close() // Cerramos el canal cuando el loop termine

		for d := range msgs {
			var msg T
			// Unmarshal del cuerpo (JSON suele ser el estándar)
			err := json.Unmarshal(d.Body, &msg)
			if err != nil {
				fmt.Printf("Error unmarshaling message: %v\n", err)
				// Si hay error, podrías usar d.Nack para re-encolar o descartar
				continue
			}

			// Ejecutar el handler con el mensaje procesado
			ackTypeResponse := handler(msg)

			// Acknowledge manual: le dice a RabbitMQ "ya lo tengo, bórralo"
			// El false significa que solo confirmamos ESTE mensaje
			// d.Ack(false)
			switch ackTypeResponse {
			case Ack:
				// Confirma la recepción exitosa. El mensaje se elimina de la cola.
				d.Ack(false)
				fmt.Println("Message acknowledged (Ack)")

			case NackRequeue:
				// Rechaza el mensaje y solicita que se vuelva a poner en la cola (requeue).
				d.Nack(false, true)
				fmt.Println("Message nacked and requeued")

			case NackDiscard:
				// Rechaza el mensaje y no lo vuelve a encolar. Se elimina o va a un DLX.
				d.Nack(false, false)
				fmt.Println("Message nacked and discarded")
			}
			fmt.Print(" >")
		}
	}()

	return nil
}
