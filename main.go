package main

import (
	"fmt"
	"time"

	"encoding/json"

	"github.com/rs/zerolog/log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Logger.Printf("%s: %s", msg, err)
	}
}

const (
	maxQueueSize   = 100000
	maxWorkerCount = 2
	batchSize      = 100 // Number of rows per batch
)

func main() {

	MysqlDbConnect()

	conn, err := amqp.Dial("amqp://liden:lID3n@10.132.0.28:5672/")
	//conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

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

	// Set QoS for the channel (prefetch_count = 1)
	err = ch.Qos(
		400,   // prefetch_count
		0,     // prefetch_size
		false, // global (false means QoS is applied per channel, not globally)
	)
	if err != nil {
		fmt.Println("Failed to set QoS: ", err.Error())
	}

	// Create a blocking queue (channel) for the messages
	queue := make(chan Odds, maxQueueSize)

	// Start worker goroutines to process database inserts in batches
	for i := 0; i < maxWorkerCount; i++ {
		go worker(queue)
	}

	msgs, err := ch.Consume(
		q.Name,            // queue
		"MARKET_CONSUMER", // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	failOnError(err, "Failed to register a consumer")

	// Start the consumer
	consumeFromRabbitMQ(msgs, queue)

	// for msg := range msgs {
	// 	go processMessage(msg)
	// }

	fmt.Println("Waiting for messages. CRTL+C to exit")
	select {}

	//marketsConsumer(conn)
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

type Selections []struct {
	Id            int    `json:"Id"`
	TradingStatus string `json:"TradingStatus"`
	Name          string `json:"Name"`
	Range         struct {
		High int `json:"High"`
		Low  int `json:"Low"`
	} `json:"Range"`
	Numerator   float64 `json:"Numerator"`
	Denominator float64 `json:"Denominator"`
	Decimal     float64 `json:"Decimal"`
}

type Outcome_obj struct {
	Market_id    int     `json:"market_id"`
	Outcome_id   int     `json:"outcome_id"`
	Outcome_name string  `json:"outcome_name"`
	Alias        string  `json:"alias"`
	Odds         float64 `json:"odds"`
	Odd_status   string  `json:"odd_status"`
}

type Highlights_market struct {
	Market_id int    `json:"market_id"`
	Specifier string `json:"specifier"`
	Name      string `json:"name"`
	Alias     string `json:"alias"`
	Priority  int    `json:"priority"`
}

type Odds struct {
	Outcome_id        int     `json:"outcome_id"`
	Odd_status        string  `json:"odd_status"`
	Outcome_name      string  `json:"outcome_name"`
	Match_id          int     `json:"match_id"`
	Odds              float64 `json:"odds"`
	Prevous_odds      float64 `json:"prevous_odds"`
	Direction         float64 `json:"direction"`
	Producer_name     string  `json:"producer_name"`
	Market_id         int     `json:"market_id"`
	Producer_id       int     `json:"producer_id"`
	Producer_status   int     `json:"producer_status"`
	Market_name       string  `json:"market_name"`
	Time_stamp        string  `json:"time_stamp"`
	Processing_delays int     `json:"processing_delays"`
	Status            bool    `json:"status"`
	Status_name       string  `json:"status_name"`
	Alias             string  `json:"alias"`
}

// func batchInsertOddslive(records []Odds) error {
// 	// Prepare the batch insert query template
// 	stmt, err := Db.Prepare("INSERT INTO odds_live (outcome_id, odd_status, outcome_name, match_id, odds, prevous_odds, direction, producer_name, market_id, producer_id, producer_status, market_name, time_stamp, processing_delays, status, status_name, alias) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE odd_status=VALUES(odd_status), odds=VALUES(odds), prevous_odds=VALUES(prevous_odds), producer_id=VALUES(producer_id), alias=VALUES(alias), market_name=VALUES(market_name), status=VALUES(status), status_name=VALUES(status_name), odd_status=VALUES(odd_status)")
// 	if err != nil {
// 		fmt.Println("Error preparing statement: ", err)
// 	}
// 	defer stmt.Close()

// 	// Start a transaction
// 	tx, err := Db.Begin()
// 	if err != nil {
// 		fmt.Println("Error starting transaction: ", err)
// 	}
// 	defer tx.Rollback() // Rollback if the function returns an error

// 	// Execute the batch insert in chunks (e.g., 100 records per chunk)
// 	batchSize := 200
// 	for i := 0; i < len(records); i += batchSize {
// 		end := i + batchSize
// 		if end > len(records) {
// 			end = len(records)
// 		}

// 		// Prepare a batch insert for this chunk of records
// 		args := make([]interface{}, 0, (end-i)*2)

// 		for _, record := range records[i:end] {
// 			args = append(args, record.Outcome_id, record.Odd_status, record.Outcome_name, record.Match_id,
// 				record.Odds, record.Prevous_odds, record.Direction, record.Producer_name, record.Market_id,
// 				record.Producer_id, record.Producer_status, record.Market_name,
// 				record.Time_stamp, record.Processing_delays, record.Status, record.Status_name, record.Alias)
// 		}

// 		// Execute the batch insert
// 		_, err := tx.Stmt(stmt).Exec(args...)
// 		if err != nil {
// 			fmt.Println("Error executing Odds batch insert: ", err)
// 		}
// 	}

// 	// Commit the transaction
// 	if err := tx.Commit(); err != nil {
// 		fmt.Println("Error committing markets transaction: ", err)
// 	}

// 	//fmt.Printf("Inserted Odds %d records successfully.\n", len(records))
// 	return nil
// }

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

	// Set QoS for the channel (prefetch_count = 1)
	err = ch.Qos(
		400,   // prefetch_count
		0,     // prefetch_size
		false, // global (false means QoS is applied per channel, not globally)
	)
	if err != nil {
		fmt.Println("Failed to set QoS: ", err.Error())
	}

	// Create a blocking queue (channel) for the messages
	queue := make(chan Odds, maxQueueSize)

	// Start worker goroutines to process database inserts in batches
	for i := 0; i < maxWorkerCount; i++ {
		go worker(queue)
	}

	msgs, err := ch.Consume(
		q.Name,            // queue
		"MARKET_CONSUMER", // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	failOnError(err, "Failed to register a consumer")

	// Start the consumer
	consumeFromRabbitMQ(msgs, queue)

	// for msg := range msgs {
	// 	go processMessage(msg)
	// }

	fmt.Println("Waiting for messages. CRTL+C to exit")
	select {}

}

// Insert batched messages into MySQL DB
func insertBatchIntoDB(messages []Odds) error {
	// Start building the INSERT query
	//query := "INSERT INTO odds_live (outcome_id, odd_status, outcome_name, match_id, odds, prevous_odds, direction, producer_name, market_id, producer_id, producer_status, market_name, time_stamp, processing_delays, status, status_name, alias) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE odd_status=VALUES(odd_status), odds=VALUES(odds), prevous_odds=VALUES(prevous_odds), producer_id=VALUES(producer_id), alias=VALUES(alias), market_name=VALUES(market_name), status=VALUES(status), status_name=VALUES(status_name), odd_status=VALUES(odd_status)"
	query := "INSERT INTO odds_live (outcome_id, odd_status, outcome_name, match_id, odds, prevous_odds, direction, producer_name, market_id, producer_id, producer_status, market_name, time_stamp, processing_delays, status, status_name, alias) VALUES "
	vals := []interface{}{}

	// Add each message's data into the query
	for _, record := range messages {
		query += "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?),"
		vals = append(vals, record.Outcome_id, record.Odd_status, record.Outcome_name, record.Match_id,
			record.Odds, record.Prevous_odds, record.Direction, record.Producer_name, record.Market_id,
			record.Producer_id, record.Producer_status, record.Market_name,
			record.Time_stamp, record.Processing_delays, record.Status, record.Status_name, record.Alias)
	}

	// Remove the last comma
	query = query[:len(query)-1]

	// Add "ON DUPLICATE KEY UPDATE" to the query
	query += " ON DUPLICATE KEY UPDATE odd_status=VALUES(odd_status), odds=VALUES(odds), prevous_odds=VALUES(prevous_odds), producer_id=VALUES(producer_id), alias=VALUES(alias), market_name=VALUES(market_name), status=VALUES(status), status_name=VALUES(status_name), odd_status=VALUES(odd_status) "

	// Execute the query
	_, err := Db.Exec(query, vals...)
	return err
}

// RabbitMQ Consumer
func consumeFromRabbitMQ(msgs <-chan amqp.Delivery, queue chan Odds) {

	for msg := range msgs {

		ackStartTime := time.Now()

		// Manually acknowledge the message
		if err := msg.Ack(false); err != nil {
			log.Printf("Failed to acknowledge message: %v", err.Error())
		} else {
			fmt.Println("Message acknowledged.")
			log.Printf(" [x] %s", msg.Body)
		}

		// Record the time after the work is done
		ackStopTime := time.Now()

		// Calculate the difference between the two times
		ackStartDuration := ackStopTime.Sub(ackStartTime)

		// Print the time difference
		fmt.Printf("Acknowledgement Time taken: %v\n", ackStartDuration)

		var marketSet MarketSet

		if err := json.Unmarshal(msg.Body, &marketSet); err != nil {
			fmt.Println(err)
		}

		// Record the current time
		startTime := time.Now()

		for _, markets := range marketSet.Markets {

			for _, selections := range markets.Selections {
				odds, err := json.Marshal(markets.Selections)
				if err != nil {
					fmt.Println("Selections not found")
				}

				// Saving individual Selections
				var unmarshalselections Selections

				err = json.Unmarshal([]byte(odds), &unmarshalselections)
				if err != nil {
					panic(err.Error())
				}

				for _, vals := range unmarshalselections {

					var alias string
					var market_name_alias string

					if markets.Name == "Match Result" {
						if vals.Name == markets.Selections[0].Name {
							alias = "1"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "x"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "2"
						} else {
							alias = selections.Name
						}
					}

					if markets.Name == "Match Result" {
						market_name_alias = "1x2"
					} else {
						market_name_alias = markets.MarketType.Name
					}

					t, err := time.Parse(time.RFC3339, markets.ExpiryUtc)
					if err != nil {
						fmt.Println(err)
					}

					loc, err := time.LoadLocation("Africa/Nairobi")
					if err != nil {
						fmt.Println(err)
					}
					mstTime := t.In(loc)

					dateVal := mstTime.Format(time.DateTime)

					odd := Odds{
						Outcome_id:        vals.Id,
						Odd_status:        vals.TradingStatus,
						Outcome_name:      vals.Name,
						Match_id:          marketSet.FixtureId,
						Odds:              vals.Decimal,
						Prevous_odds:      vals.Decimal,
						Direction:         selections.Range.High,
						Producer_name:     markets.MarketType.Name,
						Market_id:         markets.MarketType.Id,
						Producer_id:       vals.Id,
						Producer_status:   1,
						Market_name:       market_name_alias,
						Time_stamp:        dateVal,
						Processing_delays: 1,
						Status:            markets.InPlay,
						Status_name:       markets.TradingStatus,
						Alias:             alias,
					}

					select {
					case queue <- odd: //Send message to queue
					// Message successfully added to queue
					default: //If queue is full, drop the message. Slows down the rate at which messages are consumed
						// Backpressure Implementation
						// Add a small delay to avoid flooding the queue
						time.Sleep(100 * time.Millisecond)

						fmt.Println("Queue is full, dropping message")
					}
				}
			}
		}

		// Record the time after the work is done
		endTime := time.Now()

		// Calculate the difference between the two times
		duration := endTime.Sub(startTime)

		// Print the time difference
		fmt.Printf("DB insertion time for Markets: %v\n", duration)

	}
}

// Worker that takes messages from the queue and inserts them into DB in batches
func worker(queue chan Odds) {
	var batch []Odds

	for msg := range queue {
		// Add message to batch
		batch = append(batch, msg)

		// If batch is full, insert it into DB
		if len(batch) >= batchSize {
			if err := insertBatchIntoDB(batch); err != nil {
				fmt.Println("Error inserting batch odds into DB:", err)
			} else {
				fmt.Printf("Inserted %d messages\n", len(batch))
			}
			// Reset batch for next group of messages
			batch = nil
		}
	}

	// Insert any remaining messages after the loop
	if len(batch) > 0 {
		if err := insertBatchIntoDB(batch); err != nil {
			fmt.Println("Error inserting final batch into DB:", err)
		} else {
			fmt.Printf("Inserted remaining %d messages\n", len(batch))
		}
	}
}

// func processMessage(msg amqp.Delivery) {

// 	ackStartTime := time.Now()

// 	// Manually acknowledge the message
// 	if err := msg.Ack(false); err != nil {
// 		log.Printf("Failed to acknowledge message: %v", err)
// 	} else {
// 		fmt.Println("Message acknowledged.")
// 		log.Printf(" [x] %s", msg.Body)
// 	}

// 	// Record the time after the work is done
// 	ackStopTime := time.Now()

// 	// Calculate the difference between the two times
// 	ackStartDuration := ackStopTime.Sub(ackStartTime)

// 	// Print the time difference
// 	fmt.Printf("Acknowledgement Time taken: %v\n", ackStartDuration)

// 	var marketSet MarketSet

// 	if err := json.Unmarshal(msg.Body, &marketSet); err != nil {
// 		fmt.Println(err)
// 	}

// 	// Record the current time
// 	startTime := time.Now()

// 	for _, markets := range marketSet.Markets {

// 		highlights_market := []Highlights_market{
// 			{markets.MarketType.Id, markets.TradingStatus, markets.MarketType.Name, markets.MarketType.Name, 1},
// 		}

// 		batchInsert(highlights_market)

// 		for _, selections := range markets.Selections {
// 			odds, err := json.Marshal(markets.Selections)
// 			if err != nil {
// 				fmt.Println("Selections not found")
// 			}

// 			// Saving individual Selections
// 			var unmarshalselections Selections

// 			err = json.Unmarshal([]byte(odds), &unmarshalselections)
// 			if err != nil {
// 				panic(err.Error())
// 			}

// 			for _, vals := range unmarshalselections {

// 				var alias string
// 				var market_name_alias string

// 				if markets.Name == "Match Result" {
// 					if vals.Name == markets.Selections[0].Name {
// 						alias = "1"
// 					} else if vals.Name == markets.Selections[1].Name {
// 						alias = "x"
// 					} else if vals.Name == markets.Selections[2].Name {
// 						alias = "2"
// 					} else {
// 						alias = selections.Name
// 					}
// 				}

// 				if markets.Name == "Match Result" {
// 					market_name_alias = "1x2"
// 				} else {
// 					market_name_alias = markets.MarketType.Name
// 				}

// 				t, err := time.Parse(time.RFC3339, markets.ExpiryUtc)
// 				if err != nil {
// 					fmt.Println(err)
// 				}

// 				loc, err := time.LoadLocation("Africa/Nairobi")
// 				if err != nil {
// 					fmt.Println(err)
// 				}
// 				mstTime := t.In(loc)

// 				dateVal := mstTime.Format(time.DateTime)

// 				odds := []Odds{
// 					{vals.Id, vals.TradingStatus, vals.Name, marketSet.FixtureId, vals.Decimal, vals.Decimal, selections.Range.High, markets.MarketType.Name, markets.MarketType.Id, vals.Id, 1, market_name_alias, dateVal, 1, markets.InPlay, markets.TradingStatus, alias},
// 				}

// 				oddStartTime := time.Now()

// 				// Calculate the difference between the two times

// 				batchInsertOddslive(odds)

// 				oddStopTime := time.Now()

// 				oddsInsertDuration := oddStopTime.Sub(oddStartTime)

// 				// Print the time difference
// 				fmt.Println("Odds insertion Time taken: ", oddsInsertDuration, " Started at: ", oddStartTime, " Finished at: ", oddStopTime)
// 			}
// 		}
// 	}

// 	// Record the time after the work is done
// 	endTime := time.Now()

// 	// Calculate the difference between the two times
// 	duration := endTime.Sub(startTime)

// 	// Print the time difference
// 	fmt.Printf("DB insertion time for Markets: %v\n", duration)
// }
