package api

import (
	"project_sem/internal/db"

	"github.com/gorilla/mux"
)

func NewRouter(pg *db.PG) *mux.Router {
	r := mux.NewRouter()

	api := r.PathPrefix("/api/v0").Subrouter()

	api.HandleFunc("/prices", PostPrices(pg)).Methods("POST")
	api.HandleFunc("/prices", GetPrices(pg)).Methods("GET")

	return r
}
