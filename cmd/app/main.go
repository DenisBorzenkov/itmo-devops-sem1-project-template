package main

import (
	"log"
	"net/http"
	"os"

	"project_sem/internal/api"
	"project_sem/internal/db"
)

func main() {
	pg, err := db.NewPostgres(db.PGConfig{
		Host:     os.Getenv("DB_HOST"),
		Port:     5432,
		User:     os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASSWORD"),
		DBName:   os.Getenv("DB_NAME"),
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
