package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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

	fmt.Println("Intentando declarar cola y exchange...")

	// --- PASO 1: Asegurar que los Exchanges existan ---
	// Declaramos el exchange principal
	// Usamos un pequeño check para saber si es 'direct' o 'topic'
	kind := "topic"
	if exchange == routing.ExchangePerilDirect {
		kind = "direct"
	}

	err = ch.ExchangeDeclare(exchange, kind, true, false, false, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare exchange %s: %v", exchange, err)
	}

	// Declaramos el Dead Letter Exchange (peril_dlx)
	// Suele ser de tipo 'fanout' para enviar el mensaje fallido a todas las colas conectadas a él
	err = ch.ExchangeDeclare("peril_dlx", "fanout", true, false, false, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare dlx: %v", err)
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
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		unmashalJSON)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		unmarshalGOB)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
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

	fmt.Printf("DEBUG: Consumidor registrado en la cola: %s\n", queueName)

	fmt.Println("DEBUG: Consumidor iniciado con éxito, esperando mensajes...")

	go func() {
		defer ch.Close() // Cerramos el canal cuando el loop termine

		for d := range msgs {
			var msg T
			// Unmarshal del cuerpo (JSON suele ser el estándar)
			// err := json.Unmarshal(d.Body, &msg)
			msg, err := unmarshaller(d.Body)
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
			fmt.Print("> ")
		}
	}()

	return nil
}

func unmashalJSON[T any](data []byte) (T, error) {
	var msg T
	err := json.Unmarshal(data, &msg)
	if err != nil {
		fmt.Printf("Error unmarshaling message: %v\n", err)
		return msg, err
	}
	return msg, nil
}

func unmarshalGOB[T any](data []byte) (T, error) {
	var msg T
	dataIO := bytes.NewReader(data)
	decoder := gob.NewDecoder(dataIO)
	err := decoder.Decode(&msg)
	if err != nil {
		fmt.Printf("¡ERROR DE GOB!: %v\n", err)
		return msg, err
	}
	return msg, nil
}
