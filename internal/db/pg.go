package db

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type PGConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

type PG struct {
	DB *sql.DB
}

const (
	CREATE_TABLE_QUERY = `CREATE TABLE IF NOT EXISTS prices (
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	category TEXT NOT NULL,
	price NUMERIC(10,2) NOT NULL,
	create_date DATE NOT NULL,

	CONSTRAINT unique_price_row UNIQUE (name, category, price, create_date)
);`
)

func NewPostgres(cfg PGConfig) (*PG, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName, cfg.SSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	_, err = db.Exec(CREATE_TABLE_QUERY)
	if err != nil {
		return nil, err
	}

	return &PG{DB: db}, nil
}

func (p *PG) Close() {
	_ = p.DB.Close()
}
