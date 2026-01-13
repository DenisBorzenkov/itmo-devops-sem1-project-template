package db

import (
	"fmt"
	"project_sem/internal/models"
	"strings"
	"time"
)

const (
	insertPriceQuery = `INSERT INTO prices (id, name, category, price, create_date)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT DO NOTHING;`
	selectTotalPriceQuery      = `SELECT SUM(price) FROM prices;`
	selectTotalCategoriesQuery = `SELECT COUNT(DISTINCT category) FROM prices;`
)

type InsertPricesStats struct {
	InsCount int
	DupCount int
}

func InsertPrices(pg *PG, prices []models.Price) (InsertPricesStats, error) {
	stats := InsertPricesStats{}

	tx, err := pg.DB.Begin()
	if err != nil {
		return stats, fmt.Errorf("begin transaction: %v", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(insertPriceQuery)
	if err != nil {
		return stats, fmt.Errorf("prepare insert: %v", err)
	}
	defer stmt.Close()

	for _, price := range prices {
		res, err := stmt.Exec(price.ID, price.Name, price.Category, price.Price, price.CreatedAt)
		if err != nil {
			return stats, fmt.Errorf("insert price: %v", err)
		}

		affected, err := res.RowsAffected()
		if err != nil {
			return stats, fmt.Errorf("rows affected: %v", err)
		}

		if affected > 0 {
			stats.InsCount++
		} else {
			stats.DupCount++
		}
	}

	if err = tx.Commit(); err != nil {
		return stats, fmt.Errorf("commit: %v", err)
	}

	return stats, nil
}

func SelectTotalPrice(pg *PG) (float64, error) {
	var sum float64
	if err := pg.DB.QueryRow(selectTotalPriceQuery).Scan(&sum); err != nil {
		return 0, fmt.Errorf("SelectTotalPrice: %v", err)
	}
	return sum, nil
}

func SelectTotalCategories(pg *PG) (int, error) {
	var count int
	if err := pg.DB.QueryRow(selectTotalCategoriesQuery).Scan(&count); err != nil {
		return 0, fmt.Errorf("SelectTotalCategories: %v", err)
	}
	return count, nil
}

func SelectByFilter(pg *PG, start, end *time.Time, min, max *int) ([]models.Price, error) {
	query := `SELECT id, name, category, price, create_date FROM prices`
	where := []string{}
	args := []any{}
	argIdx := 1

	if start != nil && end != nil {
		where = append(where, fmt.Sprintf("create_date BETWEEN $%d AND $%d", argIdx, argIdx+1))
		args = append(args, *start, *end)
		argIdx += 2
	} else if start != nil {
		where = append(where, fmt.Sprintf("create_date >= $%d", argIdx))
		args = append(args, *start)
		argIdx++
	} else if end != nil {
		where = append(where, fmt.Sprintf("create_date <= $%d", argIdx))
		args = append(args, *end)
		argIdx++
	}

	if min != nil && max != nil {
		where = append(where, fmt.Sprintf("price BETWEEN $%d AND $%d", argIdx, argIdx+1))
		args = append(args, *min, *max)
		argIdx += 2
	} else if min != nil {
		where = append(where, fmt.Sprintf("price >= $%d", argIdx))
		args = append(args, *min)
		argIdx++
	} else if max != nil {
		where = append(where, fmt.Sprintf("price <= $%d", argIdx))
		args = append(args, *max)
		argIdx++
	}

	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}

	rows, err := pg.DB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("SelectByFilter query: %v", err)
	}
	defer rows.Close()

	var prices []models.Price

	for rows.Next() {
		var p models.Price
		if err := rows.Scan(&p.ID, &p.Name, &p.Category, &p.Price, &p.CreatedAt); err != nil {
			return prices, fmt.Errorf("SelectByFilter scan: %v", err)
		}
		prices = append(prices, p)
	}

	if err = rows.Err(); err != nil {
		return prices, fmt.Errorf("SelectByFilter rows: %v", err)
	}

	return prices, nil
}
