package main

import (
	"database/sql"
	"fmt"
	"github.com/hayatochiri/pit-organ"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/xerrors"
	"time"
)

func main() {
	connection := &pitOrgan.Connection{
		Token:       "token",
		Environemnt: pitOrgan.OandaPractice,
		Timeout:     time.Second * 10,
	}

	fmt.Printf("%+v\n", connection)

	db, err := sql.Open("sqlite3", "./oanda.db")
	if err != nil {
		panic(xerrors.Errorf("Open SQLite3 DB failed: %w", err))
	}

	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS "BOOKS" ("ID" INTEGER PRIMARY KEY, "TITLE" VARCHAR(255))`,
	)
	if err != nil {
		panic(xerrors.Errorf("Create books table failed: %w", err))
	}

	fmt.Print("Done")
}
