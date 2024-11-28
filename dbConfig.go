package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

var Db *sql.DB

func MysqlDbConnect() {
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
		log.Fatal(err.Error())
	}

	pingErr := Db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	fmt.Println("Connected to MySQL DB.")

}
