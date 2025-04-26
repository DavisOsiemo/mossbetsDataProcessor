package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	amqp "github.com/rabbitmq/amqp091-go"
)

var messagesEnqueued int64
var lastThroughputTime time.Time

func isDeadlockError(err error) bool {

	if err == nil {
		return false
	}

	if sqlErr, ok := err.(*mysql.MySQLError); ok {
		return sqlErr.Number == 1213
	}
	return false
}

// queue inflow rate
func trackThroughput() {
	now := time.Now()
	if now.Sub(lastThroughputTime).Seconds() >= 1.0 {
		throughput := float64(messagesEnqueued)

		log.Info().Float64("inflow throughput", throughput).Msg("Messages/sec throughput")

		lastThroughputTime = now
		messagesEnqueued = 0
	}
}

func consumeFromRabbitMQ(ch *amqp.Channel, queue chan Odds, consumerTag string, maxQueueSize int) {
	q, err := ch.QueueDeclare(
		"MARKETS_QUEUE",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name,      // queue
		consumerTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	failOnError(err, "Failed to register a consumer")

	for msg := range msgs {
		go processMessage(msg, queue, maxQueueSize)
	}
}

func processMessage(msg amqp.Delivery, queue chan Odds, maxQueueSize int) {

	if err := msg.Ack(false); err != nil {
		log.Printf("Failed to acknowledge message: %v", err.Error())
	} else {
		log.Printf(" [x] %s", msg.Body)
	}

	var marketSet MarketSet

	if err := json.Unmarshal(msg.Body, &marketSet); err != nil {
		fmt.Println(err)
	}

	startTime := time.Now()

	aliasPriorityMap := map[string]int{
		"1":  100,
		"x":  70,
		"2":  40,
		"1X": 38,
		"12": 35,
		"X2": 32,
	}

	for _, markets := range marketSet.Markets {

		for _, selections := range markets.Selections {

			odds, err := json.Marshal(markets.Selections)
			if err != nil {
				fmt.Println("Selections not found")
			}

			var unmarshalselections Selections

			err = json.Unmarshal([]byte(odds), &unmarshalselections)
			if err != nil {
				panic(err.Error())
			}

			for _, vals := range unmarshalselections {

				var alias string
				var marketNameAlias string
				var outcome_name string
				var alias_priority int

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

				switch markets.MarketType.Id {
				case 2:
					marketNameAlias = "1x2"
				default:
					marketNameAlias = markets.Name
				}

				if markets.Name == "Match Result" {
					if vals.Name == markets.Selections[0].Name {
						alias = "1"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "x"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[2].Name {
						alias = "2"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Double Chance" {
					if vals.Name == markets.Selections[0].Name {
						alias = "1X"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "12"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[2].Name {
						alias = "X2"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Half-time Double Chance" {
					if vals.Name == markets.Selections[0].Name {
						alias = "1X"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "12"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[2].Name {
						alias = "X2"
						outcome_name = vals.Name

					}
				} else if markets.Name == "Half-time Result" {
					if vals.Name == markets.Selections[0].Name {
						alias = "1"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "x"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[2].Name {
						alias = "2"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Match Result (Excluding Overtime)" {
					if vals.Name == markets.Selections[0].Name {
						alias = "1"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "x"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[2].Name {
						alias = "2"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Both Teams To Score" {
					if vals.Name == markets.Selections[0].Name {
						alias = "Y"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "N"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Half-time Both Teams To Score" {
					if vals.Name == markets.Selections[0].Name {
						alias = "Y"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "N"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Total Goals Over / Under 1.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Total Goals Over / Under 2.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Total Goals Over / Under 3.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Total Goals Over / Under 4.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Total Goals Over / Under 5.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Half Time Total Goals Over / Under 1.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Half Time Total Goals Over / Under 2.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Half Time Total Goals Over / Under 3.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Match Result and Total Goals Over / Under 1.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "1O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "1U"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[2].Name {
						alias = "XO"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[3].Name {
						alias = "XU"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[4].Name {
						alias = "2O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[5].Name {
						alias = "2U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Match Result and Total Goals Over / Under 2.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "1O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "1U"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[2].Name {
						alias = "XO"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[3].Name {
						alias = "XU"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[4].Name {
						alias = "2O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[5].Name {
						alias = "2U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Match Result and Total Goals Over / Under 3.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "1O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "1U"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[2].Name {
						alias = "XO"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[3].Name {
						alias = "XU"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[4].Name {
						alias = "2O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[5].Name {
						alias = "2U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Match Result and Total Goals Over / Under 4.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "1O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "1U"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[2].Name {
						alias = "XO"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[3].Name {
						alias = "XU"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[4].Name {
						alias = "2O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[5].Name {
						alias = "2U"
						outcome_name = vals.Name
					}
				} else if markets.Name == "Match Result and Total Goals Over / Under 5.50" {
					if vals.Name == markets.Selections[0].Name {
						alias = "1O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[1].Name {
						alias = "1U"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[2].Name {
						alias = "XO"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[3].Name {
						alias = "XU"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[4].Name {
						alias = "2O"
						outcome_name = vals.Name
					} else if vals.Name == markets.Selections[5].Name {
						alias = "2U"
						outcome_name = vals.Name
					}
				} else {
					alias = vals.Name
					outcome_name = vals.Name
				}

				alias_priority, exists := aliasPriorityMap[alias]
				if !exists {
					alias_priority = 0 // Default priority if alias is not found in the map
				}

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
					Market_name:       marketNameAlias,
					Time_stamp:        dateVal,
					Processing_delays: 1,
					Status:            markets.InPlay,
					Status_name:       markets.TradingStatus,
					Alias:             alias,
					Alias_priority:    alias_priority,
				}

				select {
				case queue <- odd:
					messagesEnqueued++
				default: //If queue is full, drop the message. Slows down the rate at which messages are consumed
					// Backpressure
					if len(queue) > maxQueueSize*3/4 && len(queue) < maxQueueSize {
						log.Warn().Msgf("Queue is more than 75%% full (current size: %d), slowing down message consumption.", len(queue))
						time.Sleep(200 * time.Millisecond) // More delay if the queue is mostly full
					} else if len(queue) > maxQueueSize/2 && len(queue) <= maxQueueSize*3/4 {
						log.Warn().Msgf("Queue is more than 50%% full (current size: %d), slowing down message consumption.", len(queue))
						time.Sleep(100 * time.Millisecond) // Medium delay for a moderately full queue
					} else if len(queue) == maxQueueSize {
						log.Warn().Msgf("Queue is full (current size: %d), slowing down message consumption.", len(queue))
						time.Sleep(50 * time.Millisecond) // More delay when the queue is full
					} else {
						log.Warn().Msgf("Queue is below 50%% full (current size: %d), slowing down message consumption.", len(queue))
						time.Sleep(50 * time.Millisecond)
					}
				}
			}
		}
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	fmt.Printf("%s Processing time: %v FixtureId: %d\n", time.Now().Format(time.RFC1123), duration, marketSet.FixtureId)

	trackThroughput()

}

func worker(queue chan Odds, workerID int, batchSize int) {

	var batch []Odds

	var messageCount int
	var startTime time.Time
	var lastReportTime time.Time

	for msg := range queue {

		if messageCount == 0 {
			startTime = time.Now()
			lastReportTime = startTime
		}

		batch = append(batch, msg)
		messageCount++

		// worker performance
		now := time.Now()
		if now.Sub(lastReportTime).Seconds() >= 1.0 {

			throughput := float64(messageCount) / now.Sub(startTime).Seconds()

			log.Info().Int("worker_id", workerID).Float64("worker throughput", throughput).
				Msg("Processed messages/sec in the last second")

			lastReportTime = now
		}

		if len(batch) >= batchSize {
			if err := insertBatchIntoDBWithUpsert(batch); err != nil {
				fmt.Println("Error inserting batch odds into DB:", err.Error())
			} else {
				fmt.Printf("Worker %d inserted %d messages\n", workerID, len(batch))
			}
			batch = nil
		}
	}

	if len(batch) > 0 {
		if err := insertBatchIntoDBWithUpsert(batch); err != nil {
			fmt.Println("Error inserting final batch into DB:", err.Error())
		} else {
			fmt.Printf("Worker %d inserted remaining %d messages\n", workerID, len(batch))
		}
	}
}

func insertBatchIntoDBWithUpsert(messages []Odds) error {
	const maxRetries = 6
	retryCount := 0

	for {

		tx, err := Db.Begin()
		if err != nil {
			return fmt.Errorf("error starting transaction: %v", err)
		}
		defer tx.Rollback()

		query := `
			INSERT INTO odds_live (
				outcome_id, odd_status, outcome_name, match_id, odds, prevous_odds, direction, 
				producer_name, market_id, producer_id, producer_status, market_name, time_stamp, 
				processing_delays, status, status_name, alias, market_priority, alias_priority
			) VALUES `

		vals := []interface{}{}

		for _, record := range messages {
			query += "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?),"
			vals = append(vals, record.Outcome_id, record.Odd_status, record.Outcome_name, record.Match_id,
				record.Odds, record.Prevous_odds, record.Direction, record.Producer_name, record.Market_id,
				record.Producer_id, record.Producer_status, record.Market_name, record.Time_stamp,
				record.Processing_delays, record.Status, record.Status_name, record.Alias,
				record.Market_priority, record.Alias_priority)
		}

		// Remove the trailing comma
		query = query[:len(query)-1]

		query += " ON DUPLICATE KEY UPDATE " +
			"odd_status = VALUES(odd_status), " +
			"outcome_name = VALUES(outcome_name), " +
			"odds = VALUES(odds), " +
			"prevous_odds = VALUES(prevous_odds), " +
			"direction = VALUES(direction), " +
			"producer_name = VALUES(producer_name), " +
			"market_id = VALUES(market_id), " +
			"producer_id = VALUES(producer_id), " +
			"producer_status = VALUES(producer_status), " +
			"market_name = VALUES(market_name), " +
			"time_stamp = VALUES(time_stamp), " +
			"processing_delays = VALUES(processing_delays), " +
			"status = VALUES(status), " +
			"status_name = VALUES(status_name), " +
			"alias = VALUES(alias), " +
			"market_priority = VALUES(market_priority), " +
			"alias_priority = VALUES(alias_priority)"

		startTime := time.Now()
		for i := 0; i < maxRetries; i++ {
			_, err := tx.Exec(query, vals...)
			if err == nil {
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("error committing transaction: %v", err)
				}

				duration := time.Since(startTime)
				log.Printf("Query executed successfully in %v\n", duration)
				return nil
			}

			if isDeadlockError(err) {

				retryCount++
				fmt.Printf("Deadlock error encountered. Retrying (attempt %d/%d)...\n", retryCount, maxRetries)
				time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
				tx.Rollback()                                // Rollback and retry the transaction
				break
			} else {
				return fmt.Errorf("error executing batch insert: %v", err)
			}
		}

		if retryCount >= maxRetries {
			return fmt.Errorf("max retries reached for batch insert")
		}
	}
}
