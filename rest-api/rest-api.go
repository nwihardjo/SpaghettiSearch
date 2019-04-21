package main

import (
	"encoding/json"
	"log"
	"net/http"
	"context"
	"github.com/apsdehal/go-logger"
	"github.com/gorilla/mux"
	"github.com/dgraph-io/badger"
	"the-SearchEngine/parser"
	"math"
	db "the-SearchEngine/database"
)

// global declaration used in db
var forw, inv []db.DB
var ctx context.Context
var log *logger.Logger
var totalPages int32

func GetWebpages(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	query := params["terms"]
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		panic(err)
	}

	docCompressed, err := forward[1].Iterate(ctx)
	if err != nil {
		panic(err)
	}
	totalPages = len(docCompressed.KV)

	log.Debugf("Querying terms:", query)
	q := parser.Laundry(query)

	var docs []db.DocInfo
	idf := make([]float64, len(q))

	for i := 0; i < len(q) - 1; i ++ {
		v, err := inv[0].Get(ctx, q[i])
		if err == badger.ErrKeyNotFound {
			log.Debugf("Term", q[i], "does not exist in the db")
		} else if err != nil {
			log.Debugf("Error when querying inverted table", err)
			panic(err)
		}
	
		tempDocs := v.(map[string][]uint32)
		idf[i] = math.Log2(float64(totalPages) / float64(len(tempDocs)))

		for j := 0; j < len(tempDocs) - 1; j ++ {
			
		
		

	// do stuff here
	// ret should be the list of the doc returned
	json.NewEncoder(w).Encode(ret)	
}

func main() {
	// initialise db connection
	ctx, cancel := context.WithCancel(context.TODO())
	log, _ := logger.New("test", 1)
	inv, forw, err := db.DB_init(ctx, log)
	if err != nil {
		panic(err)
	}

	for _, bdb_i := range inv {
		defer bdb_i.Close(ctx, cancel)
	}
	for _, bdb := range forw {
		defer bdb.Close(ctx, cancel)
	}	
	
	// start server
	router := mux.NewRouter()
	router.HandleFunc("/query/{terms}", GetWebpages).Methods("GET")
	log.Fatal(http.ListenAndServe(":8000", router))
}
