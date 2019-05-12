package main

import (
	"database/sql"
	"github.com/davecgh/go-spew/spew"
	"github.com/hayatochiri/pit-organ"
	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/xerrors"
	"log"
	"os"
	"strings"
	"time"
)

func init() {
	err := godotenv.Load()

	if err != nil {
		panic(xerrors.Errorf("Error loading .env file: %w", err))
	}

	if os.Getenv("TOKEN") == "" {
		panic(xerrors.Errorf("Env 'TOKEN' is empty"))
	}

	if os.Getenv("ACCOUNT_ID") == "" {
		panic(xerrors.Errorf("Env 'ACCOUNT_ID' is empty"))
	}

	if e := os.Getenv("ENVIRONMENT"); e != "PRACTICE" && e != "LIVE" {
		panic(xerrors.Errorf("Env 'ENVIRONMENT' is not 'PRACTICE' or 'LIVE'"))
	}
}

func main() {
	connection := &pitOrgan.Connection{
		Token:       os.Getenv("TOKEN"),
		Environemnt: map[string]pitOrgan.OandaEnvironment{"PRACTICE": pitOrgan.OandaPractice, "LIVE": pitOrgan.OandaLive}[os.Getenv("ENVIRONMENT")],
		Timeout:     time.Second * 30,
	}

	// fmt.Printf("%+v\n", connection)

	// data, err := connection.Accounts().Get()
	// if err != nil {
	// 	log.Fatalf("%+v", xerrors.Errorf("Connection error: %w", err))
	// }
	// spew.Dump(data)

	/*
		(*pitOrgan.PriceDefinition)(0xc0001602c0)({
		 Type: (string) (len=5) "PRICE",
		 Instrument: (pitOrgan.InstrumentNameDefinition) (len=7) "USD_JPY",
		 Time: (pitOrgan.DateTimeDefinition) (len=30) "2019-05-09T13:46:34.691292714Z",
		 Status: (pitOrgan.PriceStatusDefinition) (len=9) "tradeable",
		 Tradeable: (bool) true,
		 Bids: ([]*pitOrgan.PriceBucketDefinition) (len=1 cap=4) {
		  (*pitOrgan.PriceBucketDefinition)(0xc0003ea1a0)({
		   Price: (pitOrgan.PriceValueDefinition) (len=7) "109.735",
		   Liquidity: (json.Number) (len=6) 250000
		  })
		 },
		 Asks: ([]*pitOrgan.PriceBucketDefinition) (len=1 cap=4) {
		  (*pitOrgan.PriceBucketDefinition)(0xc0003ea260)({
		   Price: (pitOrgan.PriceValueDefinition) (len=7) "109.739",
		   Liquidity: (json.Number) (len=6) 250000
		  })
		 },
		 CloseoutBid: (pitOrgan.PriceValueDefinition) (len=7) "109.716",
		 CloseoutAsk: (pitOrgan.PriceValueDefinition) (len=7) "109.758",
		 QuoteHomeConversionFactors: (*pitOrgan.QuoteHomeConversionFactorsDefinition)(<nil>),
		 UnitsAvailable: (*pitOrgan.UnitsAvailableDefinition)(<nil>)
		})
	*/

	db := initializeDB()
	RecordPriceStream(connection, db)

	log.Printf("Done\n")
}

func RecordPriceStream(conn *pitOrgan.Connection, db *sql.DB) {
	_ = decideBaseUnixTime(db)

	params := &pitOrgan.GetPricingStreamParams{
		BufferSize:  300,
		Instruments: []string{"EUR_ZAR", "EUR_PLN", "AUD_JPY", "USD_CAD", "USD_NOK", "CAD_SGD", "HKD_JPY", "NZD_JPY", "USD_HUF", "CHF_ZAR", "EUR_CZK", "AUD_HKD", "GBP_NZD", "NZD_HKD", "NZD_CHF", "USD_SAR", "GBP_CAD", "CAD_JPY", "ZAR_JPY", "NZD_SGD", "GBP_ZAR", "NZD_CAD", "USD_INR", "CAD_HKD", "SGD_CHF", "CAD_CHF", "AUD_SGD", "EUR_NOK", "EUR_CHF", "GBP_USD", "USD_MXN", "USD_CHF", "AUD_CHF", "EUR_DKK", "AUD_USD", "CHF_HKD", "USD_THB", "GBP_CHF", "TRY_JPY", "AUD_CAD", "SGD_JPY", "EUR_NZD", "USD_HKD", "EUR_AUD", "USD_DKK", "CHF_JPY", "EUR_SGD", "USD_SGD", "EUR_SEK", "USD_JPY", "EUR_TRY", "USD_CZK", "GBP_AUD", "USD_PLN", "EUR_USD", "AUD_NZD", "SGD_HKD", "EUR_HUF", "NZD_USD", "USD_CNH", "EUR_HKD", "EUR_JPY", "GBP_PLN", "GBP_JPY", "USD_TRY", "EUR_CAD", "USD_SEK", "GBP_SGD", "EUR_GBP", "GBP_HKD", "USD_ZAR"},
	}
	chs, err := conn.Accounts().AccountID(os.Getenv("ACCOUNT_ID")).Pricing().Stream().Get(params)
	if err != nil {
		log.Fatalf("%+v", xerrors.Errorf("Connection error: %w", err))
	}
	defer chs.Close()

	instrumentIDMap := make(map[pitOrgan.InstrumentNameDefinition]int)

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("%+v", xerrors.Errorf("Failed to begin transaction: %w", err))
	}
	defer tx.Commit()

	for i := 0; i < 10; i++ {
		price := <-chs.Price

		instrumentID, ok := instrumentIDMap[price.Instrument]
		if !ok {
			switch err := tx.QueryRow(`select instrument_id from instruments where instrument = "?"`, price.Instrument).Scan(&instrumentID); err {
			case nil:
			case sql.ErrNoRows:
				pr := string(price.Bids[0].Price)
				spew.Dump(pr)
				spew.Dump(strings.Index(pr, "."))
				spew.Dump(len(pr))

				// TODO: データベースに通過ペアIDが存在しない場合作成&取得
				// _, err = tx.Exec(`
				// 	insert into instruments(instrument, base_price)
				// 	                 values(?         , ?);
				// `, price.Instrument, base_price)
				// if err != nil {
				// 	log.Fatalf("%+v", xerrors.Errorf("Failed to set instrument: %w", err))
				// }
			default:
				log.Fatalf("%+v", xerrors.Errorf("Failed to get instrument_id: %w", err))
			}
			instrumentIDMap[price.Instrument] = 0
		}

		// log.Printf("Price(len=%+v): %s", len(chs.Price), spew.Sdump(price))
	}
}

func initializeDB() *sql.DB {
	db, err := sql.Open("sqlite3", "./oanda.db")
	if err != nil {
		log.Fatalf("%+v", xerrors.Errorf("Failed to open database: %w", err))
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("%+v", xerrors.Errorf("Failed to begin transaction: %w", err))
	}
	defer tx.Commit()

	_, err = tx.Exec(`
		create table if not exists price (
			unix_time unsigned big int,
			instrument_id integer,
			tradeable boolean not null,
			bid integer not null,
			ask integer not null,
			closeout_bid integer not null,
			closeout_ask integer not null,
			primary key(unix_time, instrument_id)
		);

		create table if not exists instruments (
			instrument_id integer primary key autoincrement,
			instrument character(7) not null,
			base_price integer not null
		);

		create table if not exists config (
			key text not null,
			value not null,
			primary key(key)
		);
	`)
	if err != nil {
		log.Fatalf("%+v", xerrors.Errorf("Failed to create tables: %w", err))
	}

	return db
}

func decideBaseUnixTime(db *sql.DB) int64 {
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("%+v", xerrors.Errorf("Failed to begin transaction: %w", err))
	}
	defer tx.Commit()

	var baseTime int64
	switch err := tx.QueryRow(`select value from config where key = "base_time"`).Scan(&baseTime); err {
	case nil:
		log.Printf("Get config(base_time=%v)\n", baseTime)
	case sql.ErrNoRows:
		baseTime = time.Now().Unix()
		log.Printf("Set config(base_time=%v)\n", baseTime)
		_, err = tx.Exec(`
			insert into config(key        , value)
			            values("base_time", ?    );
		`, baseTime)
		if err != nil {
			log.Fatalf("%+v", xerrors.Errorf("Failed to set bese_time: %w", err))
		}
	default:
		log.Fatalf("%+v", xerrors.Errorf("Failed to get bese_time: %w", err))
	}

	return baseTime
}

func case1() {
	db, err := sql.Open("sqlite3", "./case1.db")
	if err != nil {
		panic(xerrors.Errorf("Open SQLite3 DB failed: %w", err))
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS "PRICE" (
			"UNIX_TIME" UNSIGNED BIG INT,
			"INSTRUMENT_ID" INTEGER,
			"TRADEABLE" BOOLEAN,
			"BID" REAL,
			"ASK" REAL,
			"CLOSEOUT_BID" REAL,
			"CLOSEOUT_ASK" REAL,
			PRIMARY KEY("UNIX_TIME", "INSTRUMENT_ID")
		);

		CREATE TABLE IF NOT EXISTS "INSTRUMENTS" (
			"INSTRUMENT_ID" INTEGER,
			"INSTRUMENT" CHARACTER(7),
			PRIMARY KEY("INSTRUMENT_ID")
		);
	`)
	if err != nil {
		panic(xerrors.Errorf("Create books table failed: %w", err))
	}

	_, err = db.Exec(`BEGIN`)
	if err != nil {
		panic(xerrors.Errorf("Create books table failed: %w", err))
	}

	for i := 0; i < 10000; i++ {
		_, err = db.Exec(`
			INSERT INTO PRICE(UNIX_TIME, INSTRUMENT_ID, TRADEABLE, BID    , ASK    , CLOSEOUT_BID, CLOSEOUT_ASK)
			           VALUES(?        , 0            , TRUE     , 109.716, 109.758, 109.716     , 109.758     );
		`, i)
		if err != nil {
			log.Fatalf("%+v", xerrors.Errorf("Connection error: %w", err))
		}
	}

	_, err = db.Exec(`COMMIT`)
	if err != nil {
		panic(xerrors.Errorf("Create books table failed: %w", err))
	}
}

func case2() {
	db, err := sql.Open("sqlite3", "./case2.db")
	if err != nil {
		panic(xerrors.Errorf("Open SQLite3 DB failed: %w", err))
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS "PRICE" (
			"UNIX_TIME" UNSIGNED BIG INT,
			"INSTRUMENT_ID" INTEGER,
			"TRADEABLE" BOOLEAN,
			"BID" INTEGER,
			"ASK" INTEGER,
			"CLOSEOUT_BID" INTEGER,
			"CLOSEOUT_ASK" INTEGER,
			PRIMARY KEY("UNIX_TIME", "INSTRUMENT_ID")
		);

		CREATE TABLE IF NOT EXISTS "INSTRUMENTS" (
			"INSTRUMENT_ID" INTEGER,
			"INSTRUMENT" CHARACTER(7),
			PRIMARY KEY("INSTRUMENT_ID")
		);
	`)
	if err != nil {
		panic(xerrors.Errorf("Create books table failed: %w", err))
	}

	_, err = db.Exec(`BEGIN`)
	if err != nil {
		panic(xerrors.Errorf("Create books table failed: %w", err))
	}

	for i := 0; i < 10000; i++ {
		for j := 0; j < 71; j++ {
			_, err = db.Exec(`
				INSERT INTO PRICE(UNIX_TIME, INSTRUMENT_ID, TRADEABLE, BID    , ASK    , CLOSEOUT_BID, CLOSEOUT_ASK)
				           VALUES(?        , ?            , TRUE     , 63, 83, 63     , -83     );
			`, i, j)
			if err != nil {
				log.Fatalf("%+v", xerrors.Errorf("Connection error: %w", err))
			}
		}
	}

	_, err = db.Exec(`COMMIT`)
	if err != nil {
		panic(xerrors.Errorf("Create books table failed: %w", err))
	}
}
