package main

import (
	db "the-SearchEngine/database"
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"encoding/json"
	"log"
	"time"
)

func GetWebpages(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	log.Print("Querying ", params["terms"], "...")
	
	tempChild := []string{"children type","need","to","be","changed"}
	tempParents := []string{"parent type","need","to","be","changed"}
	temp, _ := url.Parse("https://www.google.com")
	temp_, _ := url.Parse("https://www.cse.ust.hk")
	temp1 := make(map[string]uint32)
	temp1["wordHash1"] = uint32(11)
	temp1["wordHash2"] = uint32(22)
	
	doc1 := db.DocInfo {
		Url: *temp,
		Page_title: []string{"this", "is", "google"},
		Mod_date: time.Now(),
		Page_size: uint32(10),
		Children: tempChild,
		Parents: tempParents,
		Words_mapping: temp1,
	}
	doc2 := db.DocInfo {
		Url: *temp_,
		Page_title: []string{"this", "is", "cse"},
		Mod_date: time.Now(),
		Page_size: uint32(1000), 
		Children: tempChild, 
		Parents: tempParents,
		Words_mapping: temp1,	
	}

	ret := []db.DocInfo{doc1, doc2}
	json.NewEncoder(w).Encode(ret)
}
		

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/query/{terms}", GetWebpages).Methods("GET")
	log.Fatal(http.ListenAndServe(":8080", router))
}
