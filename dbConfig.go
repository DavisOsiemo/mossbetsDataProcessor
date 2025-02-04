package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

var Db *sql.DB

func MysqlDbConnect() *sql.DB {

	// Check if the connection is already established
	if Db != nil {
		return Db
	}

	envError := godotenv.Load(".env")

	if envError != nil {
		log.Fatalf("Error loading .env file")
	}

	cfg := mysql.Config{
		User:                 os.Getenv("DB_USERNAME_PROD"),
		Passwd:               os.Getenv("DB_PASS_PROD"),
		Addr:                 os.Getenv("DB_ADDR_PROD"),
		Net:                  os.Getenv("DB_NET"),
		DBName:               os.Getenv("DB_DATABASE"),
		AllowNativePasswords: true,
	}

	// Get a database handle
	var err error

	Db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		fmt.Println(err.Error())
	}

	Db.SetMaxOpenConns(20)                  // Maximum number of open connections
	Db.SetMaxIdleConns(2)                   // Maximum number of idle connections
	Db.SetConnMaxLifetime(20 * time.Minute) // Connection lifetime (0 means no limit)

	pingErr := Db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	fmt.Println("Connected to MySQL DB.")

	return Db
}
