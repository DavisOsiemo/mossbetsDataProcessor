package main

import (
	"fmt"

	"encoding/json"

	"github.com/rs/zerolog/log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Logger.Printf("%s: %s", msg, err)
	}
}

func main() {

	conn, err := amqp.Dial("amqp://liden:lID3n@10.132.0.28:5672/")
	//conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	marketsConsumer(conn)
}

type MarketSet struct {
	FixtureId int `json:"FixtureId"`
	Markets   []struct {
		Id            int        `json:"Id"`
		Sequence      int        `json:"Sequence"`
		TradingStatus string     `json:"TradingStatus"`
		Name          string     `json:"Name"`
		ExpiryUtc     string     `json:"ExpiryUtc"`
		MarketType    MarketType `json:"MarketType"`
		InPlay        bool       `json:"InPlay"`
		Selections    []struct {
			Id            int     `json:"Id"`
			TradingStatus string  `json:"TradingStatus"`
			Name          string  `json:"Name"`
			Range         Range   `json:"Range"`
			Numerator     int     `json:"Numerator"`
			Denominator   int     `json:"Denominator"`
			Decimal       float64 `json:"Decimal"`
		} `json:"Selections"`
	} `json:"Markets"`
}

type MarketType struct {
	Id         int    `json:"Id"`
	Name       string `json:"Name"`
	IsHandicap bool   `json:"IsHandicap"`
}

type Range struct {
	High float64 `json:"High"`
	Low  float64 `json:"Low"`
}

func marketsConsumer(conn *amqp.Connection) {

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"MARKETS_QUEUE", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.ExchangeDeclare(
		"MARKETS_EXCHANGE", // name
		"direct",           // type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare an exchange: MARKETS_EXCHANGE")

	err = ch.QueueBind(
		"MARKETS_QUEUE",    // queue name
		"MARKETS_ROUTE",    // routing key
		"MARKETS_EXCHANGE", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind route: MARKETS_ROUTE")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)

			var marketSet MarketSet

			if err := json.Unmarshal(d.Body, &marketSet); err != nil {
				panic(err)
			}
			fmt.Println("This is a decoded message:::::::: ", marketSet)
		}
	}()

	log.Printf(" [*] Waiting for Market Set logs. To exit press CTRL+C")
	<-forever
}
