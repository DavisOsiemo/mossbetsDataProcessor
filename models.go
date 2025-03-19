package main

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
