package main

import (
	// "github.com/davecgh/go-spew/spew"
	// "github.com/hayatochiri/pit-organ"
	// "time"
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/xerrors"
	"log"
	"os"
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
	// connection := &pitOrgan.Connection{
	// 	Token:       os.Getenv("TOKEN"),
	// 	Environemnt: map[string]pitOrgan.OandaEnvironment{"PRACTICE": pitOrgan.OandaPractice, "LIVE": pitOrgan.OandaLive}[os.Getenv("ENVIRONMENT")],
	// 	Timeout:     time.Second * 30,
	// }

	// fmt.Printf("%+v\n", connection)

	// data, err := connection.Accounts().Get()
	// if err != nil {
	// 	log.Fatalf("%+v", xerrors.Errorf("Connection error: %w", err))
	// }
	// spew.Dump(data)

	// params := &pitOrgan.GetPricingStreamParams{
	// 	BufferSize:  300,
	// 	Instruments: []string{"USD_JPY"},
	// }
	// chs, err := connection.Accounts().AccountID(os.Getenv("ACCOUNT_ID")).Pricing().Stream().Get(params)
	// if err != nil {
	// 	log.Fatalf("%+v", xerrors.Errorf("Connection error: %w", err))
	// }

	// spew.Dump(<-chs.Price)

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

	// defer chs.Close()

	db, err := sql.Open("sqlite3", "./oanda.db")
	if err != nil {
		panic(xerrors.Errorf("Open SQLite3 DB failed: %w", err))
	}

	_, err = db.Exec(
		`
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
		`,
	)
	if err != nil {
		panic(xerrors.Errorf("Create books table failed: %w", err))
	}

	case1()

	fmt.Print("Done")
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

	for i := 0; i < 100; i++ {
		_, err = db.Exec(`
			INSERT INTO PRICE(UNIX_TIME, INSTRUMENT_ID, TRADEABLE, BID    , ASK    , CLOSEOUT_BID, CLOSEOUT_ASK)
			           VALUES(?        , 0            , TRUE     , 109.716, 109.758, 109.716     , 109.758     );
		`, i)
		if err != nil {
			log.Fatalf("%+v", xerrors.Errorf("Connection error: %w", err))
		}
	}
}
