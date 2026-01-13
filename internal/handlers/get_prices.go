package handlers

import (
	"fmt"
	"log"
	"net/http"
	"project-sem/internal/fileutils"
	"project-sem/internal/myDB"
)

func HandlerGetPrices() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Получаем данные из БД
		prices, err := myDB.GetAllPrices()
		if err != nil {
			log.Printf("DB query error: %v", err)
			http.Error(w, "Database error", http.StatusInternalServerError)
			return
		}

		csvBuffer, err := fileutils.CreateCSVFromPrices(prices)
		if err != nil {
			log.Printf("CSV creation error: %v", err)
			http.Error(w, "CSV generation error", http.StatusInternalServerError)
			return
		}

		zipBuffer, err := fileutils.CreateZipFromCSV(csvBuffer)
		if err != nil {
			log.Printf("ZIP creation error: %v", err)
			http.Error(w, "Archive error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", zipBuffer.Len()))
		
		if _, err := w.Write(zipBuffer.Bytes()); err != nil {
			log.Printf("Response write error: %v", err)
		}
	}
}
