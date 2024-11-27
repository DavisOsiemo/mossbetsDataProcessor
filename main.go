package main

import (
	"github.com/rs/zerolog/log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Logger.Printf("%s: %s", msg, err)
	}
}

func main() {

	// router := gin.Default()

	// os.Setenv("PORT", "8081")

	// if err := router.Run(fmt.Sprintf(":%s", os.Getenv("PORT"))); err != nil {
	// 	log.Fatal().Err(err).Msg("Startup failed")
	// }
	// Initialize RabbitMQ connection and channel

	conn, ch, err := initRabbitMQ()
	if err != nil {
		log.Printf("Error initializing RabbitMQ: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	marketsProcessor(ch)

	resultsProcessor(ch)

	fixturesProcessor(ch)

}

// Function to initialize the connection and channel
func initRabbitMQ() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial("amqp://liden:lID3n@10.132.0.28:5672/")
	//conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	return conn, ch, nil
}

func marketsProcessor(ch *amqp.Channel) {

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
		}
	}()

	log.Printf(" [*] Waiting for Market Set logs. To exit press CTRL+C")
	<-forever

}

func resultsProcessor(ch *amqp.Channel) {

	q, err := ch.QueueDeclare(
		"RESULTS_QUEUE", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.ExchangeDeclare(
		"RESULTS_EXCHANGE", // name
		"direct",           // type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare an exchange: MARKETS_EXCHANGE")

	err = ch.QueueBind(
		"RESULTS_QUEUE",    // queue name
		"RESULTS_ROUTE",    // routing key
		"RESULTS_EXCHANGE", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind route: RESULTS_ROUTE")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register Result Set consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for Result Set logs. To exit press CTRL+C")
	<-forever

}

func fixturesProcessor(ch *amqp.Channel) {

	q, err := ch.QueueDeclare(
		"FIXTURE_QUEUE", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.ExchangeDeclare(
		"FIXTURE_EXCHANGE", // name
		"direct",           // type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare an exchange: FOOTBALL_EXCHANGE")

	err = ch.QueueBind(
		"FIXTURE_QUEUE",    // queue name
		"FOOTBALL_ROUTE",   // routing key
		"FIXTURE_EXCHANGE", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind route: FOOTBALL_ROUTE")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a Fixtures consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for Fixtures logs. To exit press CTRL+C")
	<-forever
}
