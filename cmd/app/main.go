package main

import (
	"log"
	"net/http"
	"strconv"

	"project_sem/internal/api"
	"project_sem/internal/db"
)

const (
	dbUser     = "validator"
	dbPassword = "val1dat0r"
	dbName     = "project-sem-1"
	dbHost     = "localhost"
	dbPort     = "5432"
)

func main() {
	port, err := strconv.Atoi(dbPort)
	if err != nil {
		log.Fatalf("Invalid DB_PORT: %v", err)
	}

	pg, err := db.NewPostgres(db.PGConfig{
		Host:     dbHost,
		Port:     port,
		User:     dbUser,
		Password: dbPassword,
		DBName:   dbName,
		SSLMode:  "disable",
	})
	if err != nil {
		log.Fatalf("DB connection error: %v", err)
	}
	defer pg.Close()

	router := api.NewRouter(pg)

	log.Println("Server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}
