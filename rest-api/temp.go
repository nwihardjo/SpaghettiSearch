package main

import (
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"encoding/json"
	"log"
	"time"
)

type docResult struct {
	Url           url.URL           `json:"Url"`
	Page_title    []string          `json:"Page_title"`
	Mod_date      time.Time         `json:"Mod_date"`
	Page_size     uint32            `json:"Page_size"`
	Children      []string          `json:"Children"`
	Parents       []string          `json:"Parents"`
	Words_mapping map[string]uint32 `json:"Words_mapping"`
	PageRank      float64		`json:"PageRank"`
	FinalRank     float64		`json:"FinalRank"`
}

type request struct {
	Query	string	`json:"query"`
}

func GetWebpages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	if r.Method == "OPTIONS" {
			return
	}
	log.Print(r.Body)
	if r.Method == "POST" {
		var query request
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			panic(err)
		}

		log.Print("DEBUG: Querying ", query.Query, " ...")

		tempChild := []string{"https://www.google.com","https://www.cse.ust.hk","to","be","changed"}
		tempParents := []string{"parent type","need","to","be","changed"}
		temp, _ := url.Parse("https://www.google.com")
		temp_, _ := url.Parse("https://www.cse.ust.hk")
		temp1 := make(map[string]uint32)
		temp1["wordHash1"] = uint32(11)
		temp1["wordHash2"] = uint32(22)

		doc1 := docResult {
			Url: *temp,
			Page_title: []string{"This ", "is", " google"},
			Mod_date: time.Now(),
			Page_size: uint32(10),
			Children: tempChild,
			Parents: tempParents,
			Words_mapping: temp1,
			PageRank: 0.52341,
			FinalRank: 0.6879,
		}
		doc2 := docResult {
			Url: *temp_,
			Page_title: []string{"This ", "is ", "CSE"},
			Mod_date: time.Now(),
			Page_size: uint32(1000),
			Children: tempChild,
			Parents: tempParents,
			Words_mapping: temp1,
			PageRank: 0.841,
			FinalRank: 0.984,
		}

		ret := []docResult{doc2, doc1}
		json.NewEncoder(w).Encode(ret)
		return
	}
}


func main() {
	log.Print("hi")
	router := mux.NewRouter()
	router.HandleFunc("/query", GetWebpages)
	log.Fatal(http.ListenAndServe(":8080", router))
}
