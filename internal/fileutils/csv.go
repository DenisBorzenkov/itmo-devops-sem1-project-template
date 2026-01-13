package fileutils

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"project-sem/internal/myDB"
)

func CreateCSVFromPrices(prices []myDB.Price) (*bytes.Buffer, error) {
	var csvBuffer bytes.Buffer
	writer := csv.NewWriter(&csvBuffer)

	if err := writer.Write([]string{"id", "name", "category", "price", "create_date"}); err != nil {
		return nil, fmt.Errorf("CSV header error: %w", err)
	}

	for _, price := range prices {
		if err := writer.Write([]string{
			fmt.Sprintf("%d", price.ID),
			price.Name,
			price.Category,
			fmt.Sprintf("%.2f", price.Price),
			price.CreateDate.Format("2006-01-02 15:04:05"),
		}); err != nil {
			log.Printf("CSV write error: %v", err)
			continue
		}
	}
	writer.Flush()

	return &csvBuffer, nil
}

func CreateZipFromCSV(csvData *bytes.Buffer) (*bytes.Buffer, error) {
	var zipBuffer bytes.Buffer
	zipWriter := zip.NewWriter(&zipBuffer)
	
	dataFile, err := zipWriter.Create("data.csv")
	if err != nil {
		return nil, fmt.Errorf("ZIP create error: %w", err)
	}

	if _, err := dataFile.Write(csvData.Bytes()); err != nil {
		return nil, fmt.Errorf("ZIP write error: %w", err)
	}

	if err := zipWriter.Close(); err != nil {
		return nil, fmt.Errorf("ZIP close error: %w", err)
	}

	return &zipBuffer, nil
} 