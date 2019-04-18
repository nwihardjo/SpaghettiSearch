package main

import (
	"encoding/json"
	"log"
	"net/http"
	"github.com/gorilla/mux"
)

func GetWebpages(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	var query string
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		panic(err)
	}

	// do stuff here
	// ret should be the list of the doc returned
	json.NewEncoder(w).Encode(ret)	
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/query/{terms}", GetWebpages).Methods("GET")
	log.Fatal(http.ListenAndServe(":8000", router))
}
