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

	// Check if the connection is already established
	if Db != nil {
		return Db
	}

	// envError := godotenv.Load(".env")

	// if envError != nil {
	// 	log.Fatalf("Error loading .env file")
	// }

	// Fetch environment variables
	dbUser := os.Getenv("DB_USERNAME_PROD")
	dbPass := os.Getenv("DB_PASS_PROD")
	dbAddr := os.Getenv("DB_ADDR_PROD")
	dbNet := os.Getenv("DB_NET")
	dbName := os.Getenv("DB_DATABASE")

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
