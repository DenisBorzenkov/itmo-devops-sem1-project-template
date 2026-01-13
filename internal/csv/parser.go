package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"project_sem/internal/models"
	"strconv"
	"time"
)

func ParseCSV(r io.Reader, handle func(line int, price models.Price, err error)) error {
	reader := csv.NewReader(r)
	reader.Comma = ','
	reader.FieldsPerRecord = 5

	_, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	line := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			return nil
		}

		line++

		if err != nil {
			handle(line, models.Price{}, fmt.Errorf("read error: %w", err))
			continue
		}

		row, err := parseRow(record)
		if err != nil {
			handle(line, models.Price{}, fmt.Errorf("parse error: %w", err))
			continue
		}

		handle(line, row, nil)
	}
}

func parseRow(r []string) (models.Price, error) {
	id, err := strconv.Atoi(r[0])
	if err != nil {
		return models.Price{}, fmt.Errorf("invalid id: %s", r[0])
	}

	if len(r[1]) == 0 {
		return models.Price{}, fmt.Errorf("invalid name: %s", r[1])
	}

	if len(r[2]) == 0 {
		return models.Price{}, fmt.Errorf("invalid category: %s", r[2])
	}

	price, err := strconv.ParseFloat(r[3], 64)
	if err != nil {
		return models.Price{}, fmt.Errorf("invalid price: %s", r[3])
	}

	createdAt, err := time.Parse("2006-01-02", r[4])
	if err != nil {
		return models.Price{}, fmt.Errorf("invalid date: %s", r[4])
	}

	return models.Price{
		ID:        id,
		Name:      r[1],
		Category:  r[2],
		Price:     price,
		CreatedAt: createdAt,
	}, nil
}
