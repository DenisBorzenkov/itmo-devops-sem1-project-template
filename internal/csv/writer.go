package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"project_sem/internal/models"
)

func WriteCSV(w io.Writer, rows []models.Price) error {
	writer := csv.NewWriter(w)

	// header
	if err := writer.Write([]string{"id", "name", "category", "price", "created_at"}); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	for _, p := range rows {
		record := []string{
			fmt.Sprintf("%d", p.ID),
			p.Name,
			p.Category,
			fmt.Sprintf("%.2f", p.Price),
			p.CreatedAt.Format("2006-01-02"),
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}

	writer.Flush()
	return writer.Error()
}
