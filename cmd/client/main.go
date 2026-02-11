package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

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
		handlerMove(gs, ch))
	if err != nil {
		log.Fatalf("Could not subscribe to pause messages: %v", err)
	}

	// Subscribe to WarHandler
	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerWar(gs, ch))
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
			// fmt.Printf("Spamming not allowed yet!\n")

			if len(words) == 2 {
				amount, err := strconv.Atoi(words[1])
				if err != nil {
					fmt.Printf("Error: '%s' no es un número válido\n", words[1])
					continue
				}
				for range amount {
					logMessage := gamelogic.GetMaliciousLog()
					err := pubsub.PublishGameLog(ch, gs.GetUsername(), routing.GameLog{
						CurrentTime: time.Now().UTC(),
						Username:    gs.GetUsername(),
						Message:     logMessage,
					})
					if err != nil {
						fmt.Printf("error publishing malicious log: %s\n", err)
					}

				}
			}

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

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(ps gamelogic.ArmyMove) pubsub.AckType {
		// defer fmt.Print("> ")

		// 2. Ejecutamos la lógica de pausa del estado del juego
		mOutcome := gs.HandleMove(ps)

		switch mOutcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			// Publush a JSON to WAR

			username := gs.GetUsername()
			key := routing.WarRecognitionsPrefix + "." + username

			// 2. Crear el struct RecognitionOfWar
			recognition := gamelogic.RecognitionOfWar{
				Attacker: ps.Player,
				Defender: gs.GetPlayerSnap(),
			}

			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				key,
				recognition,
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(rOw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(dw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer log.Println("> ")

		warOutcome, winner, loser := gs.HandleWar(dw)
		fmt.Printf("Outcome: %v", warOutcome)
		logMessage := ""

		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			logMessage = fmt.Sprintf("%s, won a war against %s", winner, loser)
			// return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			logMessage = fmt.Sprintf("%s, won a war against %s", winner, loser)
			// return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			logMessage = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			// return pubsub.Ack
		default:
			fmt.Println("Error in war")
			return pubsub.NackDiscard
		}

		gLog := routing.GameLog{
			CurrentTime: time.Now().UTC(),
			Message:     logMessage,
			Username:    gs.GetUsername(),
		}

		err := pubsub.PublishGameLog(ch, dw.Attacker.Username, gLog)
		if err != nil {
			fmt.Printf("Error publishing log: %v\n", err)
			return pubsub.NackRequeue
		}

		return pubsub.Ack
	}
}
