package main

import (
	"github.com/rs/zerolog/log"

	//"log"

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

	marketsProcessor()

	resultsProcessor()

	fixturesProcessor()

}

func marketsProcessor() {

	conn, err := amqp.Dial("amqp://liden:lID3n@10.132.0.28:5672/")
	//conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"MARKETS_QUEUE", // name
		false,           // durable
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

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever

	// var forever chan struct{}

	// var upgrader = websocket.Upgrader{
	// 	ReadBufferSize:  1024,
	// 	WriteBufferSize: 1024,
	// }

	// upgrader.CheckOrigin = func(r *http.Request) bool { return true }

	// ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	return
	// }
	// defer ws.Close()

	// go func() {
	// 	for d := range msgs {
	// 		log.Printf("Received a message: %s", d.Body)
	// 		ws.WriteMessage(websocket.TextMessage, []byte(d.Body))

	// 	}
	// }()

	// log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	// <-forever
}

func resultsProcessor() {

	conn, err := amqp.Dial("amqp://liden:lID3n@10.132.0.28:5672/")
	//conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"RESULTS_QUEUE", // name
		false,           // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")

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

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

func fixturesProcessor() {

	conn, err := amqp.Dial("amqp://liden:lID3n@10.132.0.28:5672/")
	//conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	defer ch.Close()

	q, err := ch.QueueDeclare(
		"FIXTURES_QUEUE", // name
		false,            // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	failOnError(err, "Failed to declare a queue")

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

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
