package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

const (
	dbUser     = "validator"
	dbPassword = "val1dat0r"
	dbName     = "project-sem-1"
	dbHost     = "localhost"
	dbPort     = "5432"
)

type ResponseonPost struct {
	TotalItems      int     `json:"total_items"`
	TotalCategories int     `json:"total_categories"`
	TotalPrice      float64 `json:"total_price"`
	TotalCount      int     `json:"total_count"`
	DuplicatesCount int     `json:"duplicates_count"`
}

var db *sql.DB

func main() {
	var err error
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Panic("Fail to connect to DB", err)
	}
	defer db.Close()

	http.HandleFunc("/api/v0/prices", handleRequests)
	log.Println("Server has started")
	log.Panic(http.ListenAndServe(":8080", nil))
}

func handleRequests(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		handlePost(w, r)
	case "GET":
		handleGet(w, r)
	default:
		http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
	}
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	log.Println("Got POST-request.")

	archiveType := r.URL.Query().Get("type")
	if archiveType == "" {
		archiveType = "zip"
	}
	log.Printf("Archive type: %s\n", archiveType)

	file, header, err := r.FormFile("file")
	if err != nil {
		log.Printf("Fail to load the file: %v\n", err)
		http.Error(w, "Fail to load the file", http.StatusBadRequest)
		return
	}
	defer file.Close()
	log.Printf("File %s uploaded sucessfully.\n", header.Filename)

	tempDir := "./temp"
	os.MkdirAll(tempDir, os.ModePerm)

	archivePath := filepath.Join(tempDir, header.Filename)
	outFile, err := os.Create(archivePath)
	if err != nil {
		log.Printf("Fail to create outFile %s: %v\n", archivePath, err)
		http.Error(w, "Fail to create outFile", http.StatusInternalServerError)
		return
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, file)
	if err != nil {
		log.Printf("Fail to save file: %v\n", err)
		http.Error(w, "Fail to save file", http.StatusInternalServerError)
		return
	}
	log.Printf("File saved in temp dir: %s\n", archivePath)

	var totalItems int
	var totalPrice float64
	categorySet := make(map[string]bool)
	var duplicatesCount int

	switch archiveType:
	case "zip":
		zipReader, err := zip.OpenReader(archivePath)
		if err != nil {
			log.Printf("Fail to read the archive: %v\n", err)
			http.Error(w, "Fail to read the archive", http.StatusInternalServerError)
			return
		}
		defer zipReader.Close()

		for _, f := range zipReader.File {
			if strings.HasSuffix(f.Name, ".csv") {
				log.Printf("CSV detected: %s\n", f.Name)
				processCSVFromZip(f, &totalItems, &totalPrice, categorySet, &duplicatesCount)
			}
		}

	case "tar":
		tarFile, err := os.Open(archivePath)
		if err != nil {
			log.Printf("Fail to open tar file: %v\n", err)
			http.Error(w, "Fail to open tar file", http.StatusInternalServerError)
			return
		}
		defer tarFile.Close()

		tarReader := tar.NewReader(tarFile)
		for {
			header, err := tarReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Fail to read tar header: %v\n", err)
				http.Error(w, "Fail to read tar file", http.StatusInternalServerError)
				return
			}

			if strings.HasSuffix(header.Name, ".csv") {
				log.Printf("CSV detected: %s\n", header.Name)
				processCSVFromTar(tarReader, &totalItems, &totalPrice, categorySet, &duplicatesCount)
			}
		}

	default:
		http.Error(w, "Unsupported archive type. Use 'zip' or 'tar'", http.StatusBadRequest)
		return
	}

	// Получаем общее количество записей в БД
	var totalCount int
	err = db.QueryRow("SELECT COUNT(*) FROM prices").Scan(&totalCount)
	if err != nil {
		log.Printf("Failed to get total count: %v", err)
		totalCount = 0
	}

	response := ResponseonPost{
		TotalItems:      totalItems,
		TotalCategories: len(categorySet),
		TotalPrice:      totalPrice,
		TotalCount:      totalCount,
		DuplicatesCount: duplicatesCount,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("JSON error: %v", err)
		http.Error(w, "Server error", http.StatusInternalServerError)
	}

}

type DataRow struct {
	ProductID int
	CreatedAt string
	Name      string
	Category  string
	Price     float64
}

func processCSVFromZip(f *zip.File, totalItems *int, totalPrice *float64, categorySet map[string]bool, duplicatesCount *int) {
	log.Printf("Starting CSV from ZIP: %s\n", f.Name)

	rc, err := f.Open()
	if err != nil {
		log.Printf("Fail to open CSV %s: %v\n", f.Name, err)
		return
	}
	defer rc.Close()

	processCSVData(rc, totalItems, totalPrice, categorySet, duplicatesCount)
}

func processCSVFromTar(reader io.Reader, totalItems *int, totalPrice *float64, categorySet map[string]bool, duplicatesCount *int) {
	log.Println("Starting CSV from TAR")

	processCSVData(reader, totalItems, totalPrice, categorySet, duplicatesCount)
}

func processCSVData(rc io.Reader, totalItems *int, totalPrice *float64, categorySet map[string]bool, duplicatesCount *int) {
	reader := csv.NewReader(rc)

	header, err := reader.Read()
	if err != nil {
		log.Printf("Fail to read CSV header: %v\n", err)
		return
	}
	log.Printf("CSV Header: %v\n", header)

	var rows []DataRow

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Fail to read row in CSV: %v\n", err)
			return
		}

		productID, err := strconv.Atoi(strings.TrimSpace(record[0]))
		if err != nil {
			log.Printf("Invalid product_id '%s': %v\n", record[0], err)
			return
		}

		createdAt := strings.TrimSpace(record[4])
		name := strings.TrimSpace(record[1])
		category := strings.TrimSpace(record[2])

		price, err := strconv.ParseFloat(strings.TrimSpace(record[3]), 64)
		if err != nil {
			log.Printf("Invalid price '%s': %v\n", record[3], err)
			return
		}

		rows = append(rows, DataRow{
			ProductID: productID,
			CreatedAt: createdAt,
			Name:      name,
			Category:  category,
			Price:     price,
		})

		categorySet[category] = true
	}

	if len(rows) == 0 {
		log.Println("No valid rows found, skipping database insertion.")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("Fail to begin transaction: %v\n", err)
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	stmt, err := tx.Prepare("INSERT INTO prices (product_id, created_at, name, category, price) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (product_id) DO NOTHING")
	if err != nil {
		log.Printf("Fail to prepare statement: %v\n", err)
		return
	}
	defer stmt.Close()

	for _, row := range rows {
		result, err := stmt.Exec(row.ProductID, row.CreatedAt, row.Name, row.Category, row.Price)
		if err != nil {
			log.Printf("Error inserting into DB product_id %d: %v\n", row.ProductID, err)
			return
		}
		// Проверяем, была ли строка вставлена
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			*duplicatesCount++
		}
	}

	// Считаем статистику по текущей загрузке, а не по всей БД
	*totalItems += len(rows)
	for _, row := range rows {
		*totalPrice += row.Price
	}
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, created_at, name, category, price FROM prices")
	if err != nil {
		log.Printf("Failed to query database: %v\n", err)
		http.Error(w, "Error querying data", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Читаем все данные из БД в память
	var data [][]string
	for rows.Next() {
		var id int
		var createdAt, name, category string
		var price float64
		if err := rows.Scan(&id, &createdAt, &name, &category, &price); err != nil {
			log.Printf("Failed to scan row: %v\n", err)
			http.Error(w, "Error scanning data", http.StatusInternalServerError)
			return
		}
		data = append(data, []string{strconv.Itoa(id), createdAt, name, category, strconv.FormatFloat(price, 'f', -1, 64)})
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating over rows: %v\n", err)
		http.Error(w, "Error retrieving data", http.StatusInternalServerError)
		return
	}

	// Создаем CSV в памяти
	csvBuffer := &bytes.Buffer{}
	writer := csv.NewWriter(csvBuffer)
	if err := writer.Write([]string{"id", "created_at", "name", "category", "price"}); err != nil {
		log.Printf("Failed to write CSV header: %v\n", err)
		http.Error(w, "Error creating CSV", http.StatusInternalServerError)
		return
	}
	for _, row := range data {
		if err := writer.Write(row); err != nil {
			log.Printf("Failed to write CSV row: %v\n", err)
			http.Error(w, "Error creating CSV", http.StatusInternalServerError)
			return
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Printf("CSV writer error: %v\n", err)
		http.Error(w, "Error creating CSV", http.StatusInternalServerError)
		return
	}

	// Создаем ZIP в памяти
	zipBuffer := &bytes.Buffer{}
	zipWriter := zip.NewWriter(zipBuffer)
	fileInZip, err := zipWriter.Create("data.csv")
	if err != nil {
		log.Printf("Failed to create file in ZIP: %v\n", err)
		http.Error(w, "Error creating ZIP", http.StatusInternalServerError)
		return
	}
	if _, err := fileInZip.Write(csvBuffer.Bytes()); err != nil {
		log.Printf("Failed to write to ZIP: %v\n", err)
		http.Error(w, "Error creating ZIP", http.StatusInternalServerError)
		return
	}
	if err := zipWriter.Close(); err != nil {
		log.Printf("Failed to close ZIP writer: %v\n", err)
		http.Error(w, "Error creating ZIP", http.StatusInternalServerError)
		return
	}

	// Отправляем ZIP из памяти
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", strconv.Itoa(zipBuffer.Len()))
	w.Write(zipBuffer.Bytes())
}

func calculateStatistics(tx *sql.Tx) (int, int, float64, error) {
	var totalItems int
	var totalCategories int
	var totalPrice float64

	query := `
		SELECT COUNT(*), COUNT(DISTINCT category), COALESCE(SUM(price), 0)
		FROM prices
	`
	err := tx.QueryRow(query).Scan(&totalItems, &totalCategories, &totalPrice)
	if err != nil {
		return 0, 0, 0, err
	}
	return totalItems, totalCategories, totalPrice, nil
}
