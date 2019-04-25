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
	"the-SearchEngine/indexer"
	"math"
	db "the-SearchEngine/database"
	"io/ioutil"
	"sync"
)

// global declaration used in db
var forw, inv []db.DB
var ctx context.Context
var log *logger.Logger

func GetWebpages(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	query := params["terms"]
	// TODO: whether below is necessary
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		panic(err)
	}

	log.Print("Querying terms:", query)
	queryTokenised := parser.Laundry(query)

	var wg sync.WaitGroup
	chanDocHash := make(chan string)
	for term := range queryTokenised {
		wg.Add(1)
		go getQuery(chanDocHash, 

	docs := make([]db.DocInfo, 0, 50)
	docsRank := make([]float64, 0, 50)
	idf := make([]float64, len(q))
	tf := make([][]float64, len(q))
	for i := 0; i < len(q) - 1; i ++ {
		// title inverted tables
		v, err := inv[0].Get(ctx, q[i])
		if err == badger.ErrKeyNotFound {
			log.Debugf("Term", q[i], "does not exist in the db")
		} else if err != nil {
			panic(err)
		}
	
		tempDocs := v.(map[string][]uint32)
		idf[i] = math.Log2(totalDocs / float64(len(tempDocs)))

		for docHash, listPos := range tempDocs {
			tf[i]	
		

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
