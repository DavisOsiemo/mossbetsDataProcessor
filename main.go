package main

import (
	"fmt"
	"os"

	"strconv"

	"github.com/rs/zerolog/log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Logger.Printf("%s: %s", msg, err)
	}
}

func getEnvAsInt(key string, defaultValue int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		log.Info().Msgf("Environment variable %s not set, using default value %d.", key, defaultValue)
		return defaultValue
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		log.Logger.Printf("Error converting %s to int: %v. Using default value %d.", key, err, defaultValue)
		return defaultValue
	}
	return val
}

func main() {

	maxQueueSize := getEnvAsInt("MAX_QUEUE_SIZE", 80000)
	maxWorkerCount := getEnvAsInt("MAX_WORKER_COUNT", 20)
	batchSize := getEnvAsInt("BATCH_SIZE", 100)
	numConsumers := getEnvAsInt("NUM_CONSUMERS", 5)

	MysqlDbConnect()

	conn, err := amqp.Dial(os.Getenv("DB_RABBITMQ"))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"MARKETS_EXCHANGE",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare an exchange: MARKETS_EXCHANGE")

	err = ch.QueueBind(
		"MARKETS_QUEUE",
		"MARKETS_ROUTE",
		"MARKETS_EXCHANGE",
		false,
		nil)
	failOnError(err, "Failed to bind route: MARKETS_ROUTE")

	err = ch.Qos(
		100,
		0,
		false, // Per channel
	)
	if err != nil {
		fmt.Println("Failed to set QoS: ", err.Error())
	}

	queue := make(chan Odds, maxQueueSize)

	for i := 0; i < maxWorkerCount; i++ {
		go worker(queue, i+1, batchSize)
	}

	for i := 0; i < numConsumers; i++ {
		consumerTag := fmt.Sprintf("MARKET_CONSUMER_%d", i)
		go consumeFromRabbitMQ(ch, queue, consumerTag, maxQueueSize)
	}

	fmt.Println("Waiting for messages. CRTL+C to exit")
	select {}

}
