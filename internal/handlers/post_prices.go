package handlers

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"project-sem/internal/myDB"
	"strings"
)

func HandlerPostPrices() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			http.Error(w, "Error parsing form: "+err.Error(), http.StatusBadRequest)
			return
		}

		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "File upload error: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		tempFile, err := os.CreateTemp("", "upload-*.zip")
		if err != nil {
			http.Error(w, "Temp file error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer os.Remove(tempFile.Name())
		defer tempFile.Close()

		if _, err = io.Copy(tempFile, file); err != nil {
			http.Error(w, "File save error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		zipReader, err := zip.OpenReader(tempFile.Name())
		if err != nil {
			http.Error(w, "ZIP read error: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer zipReader.Close()

		var csvFile *zip.File
		for _, f := range zipReader.File {
			if strings.HasSuffix(f.Name, ".csv") {
				csvFile = f
				break
			}
		}
		if csvFile == nil {
			http.Error(w, "CSV file not found", http.StatusBadRequest)
			return
		}

		rc, err := csvFile.Open()
		if err != nil {
			http.Error(w, "CSV open error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rc.Close()

		reader := csv.NewReader(rc)
		records, err := reader.ReadAll()
		if err != nil {
			http.Error(w, "CSV parse error: "+err.Error(), http.StatusBadRequest)
			return
		}

		var inputPrices []myDB.InputPrice
		for i := 1; i < len(records); i++ {
			price, err := myDB.ParseInputPrice(records[i])
			if err != nil {
				http.Error(w, "Data parsing error: "+err.Error(), http.StatusBadRequest)
				return
			}
			inputPrices = append(inputPrices, price)
		}

		totalItems, totalCategories, totalPrice, err := myDB.InsertPrices(inputPrices)
		if err != nil {
			http.Error(w, "DB insert error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		response := map[string]interface{}{
			"total_items":      totalItems,
			"total_categories": totalCategories,
			"total_price":      totalPrice,
		}

		w.Header().Set("Content-Type", "application/json")
		if err = json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "JSON encode error: "+err.Error(), http.StatusInternalServerError)
		}
	}
}
