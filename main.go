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

func main() {

	MysqlDbConnect()

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

func GroupByProperty[T any, K comparable](items []T, getProperty func(T) K) map[K][]T {
	grouped := make(map[K][]T)

	for _, item := range items {
		key := getProperty(item)
		grouped[key] = append(grouped[key], item)
	}
	return grouped
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

			for _, markets := range marketSet.Markets {

				dbResult, dbError := Db.Exec("INSERT INTO highlights_market (market_id, specifier, name, alias, priority) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE alias=?, market_id=?", markets.MarketType.Id, markets.TradingStatus, markets.MarketType.Name, markets.MarketType.Name, 1, markets.MarketType.Name, markets.MarketType.Id)
				if dbError != nil {
					log.Fatal().Err(dbError).Msg("Failed to insert MarketSet to DB")
				}
				defer dbResult.LastInsertId()

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
						fmt.Println("parsing RFC3339 time:", t)

						loc, err := time.LoadLocation("Africa/Nairobi")
						if err != nil {
							fmt.Println(err)
						}
						mstTime := t.In(loc)
						fmt.Println("UTC to Local time:", mstTime)

						dateVal := mstTime.Format(time.DateTime)

						oddsResult, oddsError := Db.Exec("INSERT INTO odds_live (outcome_id, odd_status, outcome_name, match_id, odds, prevous_odds, direction, producer_name, market_id, producer_id, producer_status, market_name, time_stamp, processing_delays, status, status_name, alias) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE odds=?, prevous_odds=?, producer_id=?, alias=?, market_name=?, status=?, status_name=?, odd_status=?", vals.Id, vals.TradingStatus, vals.Name, marketSet.FixtureId, vals.Decimal, vals.Decimal, selections.Range.High, markets.MarketType.Name, markets.MarketType.Id, vals.Id, 1, market_name_alias, dateVal, 1, markets.InPlay, markets.TradingStatus, alias, vals.Decimal, vals.Decimal, vals.Id, alias, market_name_alias, markets.InPlay, markets.TradingStatus, vals.TradingStatus)
						if oddsError != nil {
							fmt.Println("Failed to insert Odds to DB: ", oddsError.Error())
							//log.Fatal().Err(oddsError).Msg("Failed to insert Odds to DB")
						}
						defer oddsResult.LastInsertId()

						results3, err := Db.Query("SELECT odds_live.market_id, odds_live.outcome_id, odds_live.outcome_name, odds_live.alias, odds_live.odds, odds_live.odd_status FROM fixture LEFT JOIN odds_live ON fixture.match_id=odds_live.match_id WHERE fixture.match_id=? ORDER BY CASE WHEN odds_live.market_name='Match Result' then 0 else 1 end, date desc", marketSet.FixtureId)
						if err != nil {
							fmt.Println("Err", err.Error())
						}

						odd3 := []Outcome_obj{}

						for results3.Next() {
							var odd Outcome_obj

							err = results3.Scan(&odd.Market_id, &odd.Outcome_id, &odd.Outcome_name, &odd.Alias, &odd.Odds, &odd.Odd_status)

							if err != nil {
								fmt.Println(err)
							}
							odd3 = append(odd3, odd)
						}

						groupedByMarketId := GroupByProperty(odd3, func(p Outcome_obj) int {
							return p.Market_id
						})

						for market_id, group := range groupedByMarketId {
							for _, oddsObj := range group {

								oddsObjMarsh, err := json.Marshal(oddsObj)
								if err != nil {
									fmt.Println("Odds not persisted ")
								}

								outcome_id_marsh, err := json.Marshal(oddsObj.Outcome_id)
								if err != nil {
									fmt.Println("Odds not persisted ")
								}

								oddsResult, oddsError := Db.Exec("UPDATE odds_live SET oddsObject=? WHERE market_id=? AND outcome_id=?", oddsObjMarsh, market_id, outcome_id_marsh)
								if oddsError != nil {
									fmt.Println(oddsError)
								}
								defer oddsResult.LastInsertId()
							}
						}

					}

					log.Info().Msg("Market Set Added to DB " + markets.MarketType.Name + string(markets.Id))

				}
			}

			fmt.Println("This is a decoded message: ", marketSet.Markets)
		}
	}()

	log.Printf(" [*] Waiting for Market Set logs. To exit press CTRL+C")
	<-forever
}
