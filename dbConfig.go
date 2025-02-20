package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

var Db *sql.DB

func MysqlDbConnect() *sql.DB {

	if Db != nil {
		return Db
	}

	dbUser := os.Getenv("DB_USERNAME_UAT")
	dbPass := os.Getenv("DB_PASS_UAT")
	dbAddr := os.Getenv("DB_ADDR_UAT")
	dbNet := os.Getenv("DB_NET_UAT")
	dbName := os.Getenv("DB_DATABASE_UAT")

	if dbUser == "" || dbPass == "" || dbAddr == "" || dbNet == "" || dbName == "" {
		log.Fatal("Missing one or more required environment variables.")
	}

	cfg := mysql.Config{
		User:                 dbUser,
		Passwd:               dbPass,
		Addr:                 dbAddr,
		Net:                  dbNet,
		DBName:               dbName,
		AllowNativePasswords: true,
	}

	// Get a database handle
	var err error

	Db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		fmt.Println(err.Error())
	}

	Db.SetMaxOpenConns(50)                  // Maximum number of open connections
	Db.SetMaxIdleConns(2)                   // Maximum number of idle connections
	Db.SetConnMaxLifetime(20 * time.Minute) // Connection lifetime (0 means no limit)

	pingErr := Db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	fmt.Println("Connected to MySQL DB.")

	return Db
}
