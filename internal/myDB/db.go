package myDB

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

var db *sql.DB

// Price представляет структуру данных о товаре
type Price struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	Category   string    `json:"category"`
	Price      float64   `json:"price"`
	CreateDate time.Time `json:"create_date"`
}

// InputPrice представляет структуру входных данных из файла
type InputPrice struct {
	Name       string
	Category   string
	Price      float64
	CreateDate time.Time
}

// ParseInputPrice преобразует строковые данные в структуру InputPrice
func ParseInputPrice(record []string) (InputPrice, error) {
	if len(record) != 5 {
		return InputPrice{}, fmt.Errorf("incorrect number of fields: expected 5, got %d", len(record))
	}

	price, err := strconv.ParseFloat(record[3], 64)
	if err != nil {
		return InputPrice{}, fmt.Errorf("price conversion error: %w", err)
	}

	createDate, err := time.Parse("2006-01-02", record[4])
	if err != nil {
		return InputPrice{}, fmt.Errorf("date conversion error: %w", err)
	}

	return InputPrice{
		Name:       record[1],
		Category:   record[2],
		Price:      price,
		CreateDate: createDate,
	}, nil
}

// InitDB инициализирует подключение к базе данных
func InitDB() error {
	connStr := "host=localhost user=validator password=val1dat0r dbname=project-sem-1 sslmode=disable port=5432"
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS prices (
			id SERIAL PRIMARY KEY,
			name TEXT,
			category TEXT,
			price NUMERIC,
			create_date TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	if err = db.Ping(); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}
	return nil
}

func CloseDB() error {
	err := db.Close()
	if err != nil {
		return err
	}
	return nil
}

// InsertPrices вставляет данные в базу
func InsertPrices(records []InputPrice) (int, int, float64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("transaction error: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO prices(name, category, price, create_date)
		VALUES($1, $2, $3, $4)
	`)
	if err != nil {
		tx.Rollback()
		return 0, 0, 0, fmt.Errorf("prepare error: %w", err)
	}
	defer stmt.Close()

	// Вставка записей
	for _, record := range records {
		_, err = stmt.Exec(
			record.Name,
			record.Category,
			record.Price,
			record.CreateDate,
		)
		if err != nil {
			tx.Rollback()
			return 0, 0, 0, fmt.Errorf("insert error: %w", err)
		}
	}

	// Подсчет статистики
	var totalItems int
	var totalPrice float64
	var uniqueCategories int

	err = tx.QueryRow(`
		SELECT 
			COUNT(*) as total_items,
			COUNT(DISTINCT category) as unique_categories,
			COALESCE(SUM(price), 0) as total_price
		FROM prices
	`).Scan(&totalItems, &uniqueCategories, &totalPrice)
	if err != nil {
		tx.Rollback()
		return 0, 0, 0, fmt.Errorf("statistics calculation error: %w", err)
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return 0, 0, 0, fmt.Errorf("commit error: %w", err)
	}

	return totalItems, uniqueCategories, totalPrice, nil
}

// GetAllPrices возвращает все записи в виде массива структур Price
func GetAllPrices() ([]Price, error) {
	rows, err := db.Query(`
		SELECT 
			id,
			name,
			category,
			price,
			create_date
		FROM prices
		ORDER BY id
	`)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var prices []Price
	for rows.Next() {
		var p Price
		if err := rows.Scan(&p.ID, &p.Name, &p.Category, &p.Price, &p.CreateDate); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		prices = append(prices, p)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return prices, nil
}
