package main

import (
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	fmt.Println("Starting Peril client...")
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not open channel: %v", err)
	}
	defer ch.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	// pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, handlerPause())

	// _, queue, err := pubsub.DeclareAndBind(
	// 	conn,
	// 	routing.ExchangePerilDirect,
	// 	routing.PauseKey+"."+username,
	// 	routing.PauseKey,
	// 	pubsub.SimpleQueueTransient,
	// )
	// if err != nil {
	// 	log.Fatalf("could not subscribe to pause: %v", err)
	// }
	// fmt.Printf("Queue %v declared and bound!\n", queue.Name)
	//
	gs := gamelogic.NewGameState(username)
	// Subscribe to a Pause Queue
	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gs))
	if err != nil {
		log.Fatalf("Could not subscribe to pause messages: %v", err)
	}

	// Subscribe to Army Moves
	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+username,
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerMove(gs))
	if err != nil {
		log.Fatalf("Could not subscribe to pause messages: %v", err)
	}

	gamelogic.PrintClientHelp()

	for {

		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":

			err := gs.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Error crating the spawn: %v", err)
			}
			fmt.Println()
			continue
		case "move":
			move, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("Error moving: %vi\n", err)
			} else {
				fmt.Printf("Move succesfull\n")
			}
			key := routing.ArmyMovesPrefix + "." + gs.GetUsername()

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				key,
				move,
			)
			if err != nil {
				fmt.Printf("Error publishing move: %v\n", err)
				continue
			}

			fmt.Printf("Move published successfully!\n")

			continue
		case "spam":
			fmt.Printf("Spamming not allowed yet!\n")

		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "quit":
			gamelogic.PrintQuit()
			os.Exit(0)
		default:
			fmt.Println("Command not allowed")

		}

	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		// 1. Aseguramos que el prompt aparezca al finalizar el proceso
		// defer fmt.Print("> ")

		// 2. Ejecutamos la lógica de pausa del estado del juego
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(ps gamelogic.ArmyMove) pubsub.AckType {
		// 1. Aseguramos que el prompt aparezca al finalizar el proceso
		// defer fmt.Print("> ")

		// 2. Ejecutamos la lógica de pausa del estado del juego
		mOutcome := gs.HandleMove(ps)

		switch mOutcome {
		case gamelogic.MoveOutComeSafe, gamelogic.MoveOutcomeMakeWar:
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}
