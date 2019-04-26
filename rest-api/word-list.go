package mai1n

import (
	db "the-SearchEngine/database"
	"fmt"
	"context"
	"encoding/json"
	"github.com/apsdehal/go-logger"
	"github.com/gorilla/mux"
	"net/http"
	"sort"
	//"io"
	//"net/url"
	//"time"
)

var inv []db.DB
var frw []db.DB
var ctx context.Context

func GetWordList(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Getting word list...")

	pre := mux.Vars(r)["pre"]

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")

	tempT, err := inv[0].IterateInv(ctx, pre, frw[0])
	if err != nil {
		panic(err)
	}
	tempB, err := inv[1].IterateInv(ctx, pre, frw[0])
	if err != nil {
		panic(err)
	}
	merged_ := make(map[string]bool)
	for _, i := range tempT {
		merged_[i] = true
	}
	for _, i := range tempB {
		merged_[i] = true
	}
	tempT = []string{}
	tempB = []string{}
	var merged []string
	for k, _ := range merged_ {
		merged = append(merged, k)
		delete(merged_, k)
	}
	sort.Sort(sort.StringSlice(merged))
	json.NewEncoder(w).Encode(merged)
}


func m1ain() {
	ctx, cancel := context.WithCancel(context.TODO())
	log, _ := logger.New("test", 1)
	inv, frw, _ = db.DB_init(ctx, log)
	for _, bdb_i := range inv {
		defer bdb_i.Close(ctx, cancel)
	}
	for _, bdb := range frw {
		defer bdb.Close(ctx, cancel)
	}

	router := mux.NewRouter()
	router.HandleFunc("/wordlist/{pre}", GetWordList).Methods("GET")
	fmt.Println(http.ListenAndServe(":8080", router))
}
