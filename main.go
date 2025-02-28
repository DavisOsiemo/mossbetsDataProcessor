package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Logger.Printf("%s: %s", msg, err)
	}
}

const (
	maxQueueSize   = 4000
	maxWorkerCount = 2
	batchSize      = 100
)

func main() {

	MysqlDbConnect()

	conn, err := amqp.Dial(os.Getenv("DB_RABBITMQ"))
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

	fmt.Println("Waiting for messages. CRTL+C to exit")
	select {}

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
	Market_priority   int     `json:"market_priority"`
	Alias_priority    int     `json:"alias_priority"`
}

// Insert batched messages into MySQL DB
func insertBatchIntoDB(messages []Odds) error {

	// // Step 1: Collect unique match_id values from messages
	// matchIDs := make(map[int]struct{}) // Using a map to ensure uniqueness
	// for _, record := range messages {
	// 	matchIDs[record.Match_id] = struct{}{}
	// }

	// // Step 2: Check if all match_ids exist in the fixture table
	// existingMatchIDs := make(map[int]struct{})
	// query1 := "SELECT match_id FROM fixture WHERE match_id IN (?)"

	// // Convert map keys to a slice for the query
	// var matchIDSlice []interface{}
	// for matchID := range matchIDs {
	// 	matchIDSlice = append(matchIDSlice, matchID)
	// }

	// // Build the IN clause dynamically
	// inClause := "(" + strings.Repeat("?,", len(matchIDSlice)-1) + "?)"
	// query1 = strings.Replace(query1, "(?)", inClause, 1)

	// // Query the database for existing match IDs
	// rows1, err := Db.Query(query1, matchIDSlice...)
	// if err != nil {
	// 	return fmt.Errorf("error checking match_ids in fixture table: %w", err)
	// }
	// defer rows1.Close()

	// // Collect existing match IDs
	// for rows1.Next() {
	// 	var matchID int
	// 	if err := rows1.Scan(&matchID); err != nil {
	// 		return fmt.Errorf("error scanning match_id: %w", err)
	// 	}
	// 	existingMatchIDs[matchID] = struct{}{}
	// }

	// // Step 3: Validate the messages
	// for _, record := range messages {
	// 	if _, exists := existingMatchIDs[record.Match_id]; !exists {
	// 		fmt.Println("match_id does not exist in fixture table", record.Match_id)
	// 		continue
	// 	}
	// }

	// Start building the INSERT query
	query := "INSERT INTO odds_live (outcome_id, odd_status, outcome_name, match_id, odds, prevous_odds, direction, producer_name, market_id, producer_id, producer_status, market_name, time_stamp, processing_delays, status, status_name, alias, market_priority, alias_priority) VALUES "
	vals := []interface{}{}

	// Add each message's data into the query
	for _, record := range messages {
		query += "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?),"
		vals = append(vals, record.Outcome_id, record.Odd_status, record.Outcome_name, record.Match_id,
			record.Odds, record.Prevous_odds, record.Direction, record.Producer_name, record.Market_id,
			record.Producer_id, record.Producer_status, record.Market_name,
			record.Time_stamp, record.Processing_delays, record.Status, record.Status_name, record.Alias, record.Market_priority, record.Alias_priority)
	}

	// Remove the last comma
	query = query[:len(query)-1]

	// Add "ON DUPLICATE KEY UPDATE" to the query
	query += " ON DUPLICATE KEY UPDATE odd_status=VALUES(odd_status), odds=VALUES(odds), prevous_odds=VALUES(prevous_odds), producer_id=VALUES(producer_id), alias=VALUES(alias), market_name=VALUES(market_name), status=VALUES(status), status_name=VALUES(status_name), odd_status=VALUES(odd_status), market_priority=VALUES(market_priority), alias_priority=VALUES(alias_priority) "

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
					var outcome_name string
					var market_name_alias string
					var market_priority int
					var alias_priority int

					if markets.Name == "Match Result" { //1x2
						if vals.Name == markets.Selections[0].Name {
							alias = "1"
							outcome_name = "1"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "x"
							outcome_name = "x"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "2"
							outcome_name = "2"
						}
					} else if markets.Name == "Double Chance" {
						if vals.Name == markets.Selections[0].Name {
							alias = "1X"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "12"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "X2"
						}
					} else if markets.Name == "Half Time Double Chance" {
						if vals.Name == markets.Selections[0].Name {
							alias = "1X"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "12"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "X2"
						}
					} else if markets.Name == "Half-time Result" {
						if vals.Name == markets.Selections[0].Name {
							alias = "1"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "x"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "2"
						}
					} else if markets.Name == "Match Result (Excluding Overtime)" {
						if vals.Name == markets.Selections[0].Name {
							alias = "1"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "x"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "2"
						}
					} else if markets.Name == "Both Teams To Score" {
						if vals.Name == markets.Selections[0].Name {
							alias = "Y"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "N"
						}
					} else if markets.Name == "Half Time Both Teams To Score" {
						if vals.Name == markets.Selections[0].Name {
							alias = "Y"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "N"
						}
					} else if markets.Name == "Total Goals Over / Under 1.50" {
						if vals.Name == markets.Selections[0].Name {
							alias = "O"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "U"
						}
					} else if markets.Name == "Total Goals Over / Under 2.50" {
						if vals.Name == markets.Selections[0].Name {
							alias = "O"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "U"
						}
					} else if markets.Name == "Total Goals Over / Under 3.50" {
						if vals.Name == markets.Selections[0].Name {
							alias = "O"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "U"
						}
					} else if markets.Name == "Total Goals Over / Under 4.50" {
						if vals.Name == markets.Selections[0].Name {
							alias = "O"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "U"
						}
					} else if markets.Name == "Total Goals Over / Under 5.50" {
						if vals.Name == markets.Selections[0].Name {
							alias = "O"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "U"
						}
					} else if markets.Name == "Match Result and Total Goals Over / Under 1.50" {
						if vals.Name == markets.Selections[0].Name {
							alias = "1O"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "1U"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "XO"
						} else if vals.Name == markets.Selections[3].Name {
							alias = "XU"
						} else if vals.Name == markets.Selections[4].Name {
							alias = "2O"
						} else if vals.Name == markets.Selections[5].Name {
							alias = "2U"
						}
					} else if markets.Name == "Match Result and Total Goals Over / Under 2.50" {
						if vals.Name == markets.Selections[0].Name {
							alias = "1O"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "1U"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "XO"
						} else if vals.Name == markets.Selections[3].Name {
							alias = "XU"
						} else if vals.Name == markets.Selections[4].Name {
							alias = "2O"
						} else if vals.Name == markets.Selections[5].Name {
							alias = "2U"
						}
					} else if markets.Name == "Match Result and Total Goals Over / Under 3.50" {
						if vals.Name == markets.Selections[0].Name {
							alias = "1O"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "1U"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "XO"
						} else if vals.Name == markets.Selections[3].Name {
							alias = "XU"
						} else if vals.Name == markets.Selections[4].Name {
							alias = "2O"
						} else if vals.Name == markets.Selections[5].Name {
							alias = "2U"
						}
					} else if markets.Name == "Match Result and Total Goals Over / Under 4.50" {
						if vals.Name == markets.Selections[0].Name {
							alias = "1O"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "1U"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "XO"
						} else if vals.Name == markets.Selections[3].Name {
							alias = "XU"
						} else if vals.Name == markets.Selections[4].Name {
							alias = "2O"
						} else if vals.Name == markets.Selections[5].Name {
							alias = "2U"
						}
					} else if markets.Name == "Match Result and Total Goals Over / Under 5.50" {
						if vals.Name == markets.Selections[0].Name {
							alias = "1O"
						} else if vals.Name == markets.Selections[1].Name {
							alias = "1U"
						} else if vals.Name == markets.Selections[2].Name {
							alias = "XO"
						} else if vals.Name == markets.Selections[3].Name {
							alias = "XU"
						} else if vals.Name == markets.Selections[4].Name {
							alias = "2O"
						} else if vals.Name == markets.Selections[5].Name {
							alias = "2U"
						}
					} else {
						alias = vals.Name
						outcome_name = vals.Name
					}

					// Both Teams To Score
					if markets.Name == "Match Result" {
						market_name_alias = "1x2"
					} else {
						market_name_alias = markets.Name
					}

					if markets.MarketType.Id == 2 {
						market_priority = 1000
					} else if markets.MarketType.Id == 7079 {
						market_priority = 950
					} else if markets.MarketType.Id == 7202 {
						market_priority = 900
					} else if markets.MarketType.Id == 259 {
						market_priority = 850
					} else if markets.MarketType.Id == 105 {
						market_priority = 800
					} else if markets.MarketType.Id == 6832 {
						market_priority = 750
					} else if markets.MarketType.Id == 7076 {
						market_priority = 700
					} else if markets.MarketType.Id == 295 {
						market_priority = 650
					} else if markets.MarketType.Id == 10554 {
						market_priority = 600
					} else if markets.MarketType.Id == 253 {
						market_priority = 550
					} else if markets.MarketType.Id == 9498 {
						market_priority = 500
					} else if markets.MarketType.Id == 9497 {
						market_priority = 450
					} else if markets.MarketType.Id == 7086 {
						market_priority = 400
					} else if markets.MarketType.Id == 7087 {
						market_priority = 350
					} else {
						market_priority = 10
					}

					if alias == "1" {
						alias_priority = 100
					} else if alias == "x" {
						alias_priority = 70
					} else if alias == "2" {
						alias_priority = 40
					} else if alias == "1X" {
						alias_priority = 38
					} else if alias == "12" {
						alias_priority = 35
					} else if alias == "X2" {
						alias_priority = 32
					} else {
						alias_priority = 0
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
						Outcome_name:      outcome_name,
						Match_id:          marketSet.FixtureId,
						Odds:              vals.Decimal,
						Prevous_odds:      vals.Decimal,
						Direction:         selections.Range.High,
						Producer_name:     markets.MarketType.Name,
						Market_id:         markets.MarketType.Id,
						Producer_id:       markets.Id,
						Producer_status:   1,
						Market_name:       market_name_alias,
						Time_stamp:        dateVal,
						Processing_delays: 1,
						Status:            markets.InPlay,
						Status_name:       markets.TradingStatus,
						Alias:             alias,
						Market_priority:   market_priority,
						Alias_priority:    alias_priority,
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

		fmt.Println("Batch size is: ", len(batch))
		fmt.Println("Queue size is: ", len(queue))

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
