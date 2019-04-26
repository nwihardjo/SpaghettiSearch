package main1

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
	"encoding/hex"
	"crypto/md5"
)

// global declaration used in db
var forw, inv []db.DB
var ctx context.Context
var log *logger.Logger

func generateTermPipeline(listStr []string) <- chan string {
	out := make(chan string, len(listStr))
	go func() {
		for i := 0; i < len(listStr); i ++ {
			out <- listStr[i]
		}
		close(out)
	}()
	return out
}

func generateAggrDocsPipeline(docRank map[string]Rank_term) <- chan Rank_result {
	out := make(chan Rank_result, len(docRank))
	go func() {
		for docHash, rank := range docRank {
			ret := Rank_result{DocHash: docHash, }
			for titleweight := range rank.TitleWeights {
				ret.TitleRank += float64(titleweight)
			}
			for bodyweight := range rank.BodyWeights {
				ret.BodyRank += float64(bodyweight)
			}

			out <- ret
		}
		close(out)
	}()
	return out
}

// several type for easier flow of channels
type Rank_term struct (
	TitleWeights	[]float32
	BodyWeights	[]float32
)

type Rank_result struct (
	DocHash		string
	TitleRank	float64
	BodyRank	float64
)

type Rank_combined struct (
	DocHash	string
	Rank	float64
)

func getFromInverted(ctx context.Context, termChan <-chan string, inv []db.DB) <-chan map[string]Rank_term {
	out := make(chan map[string]Rank_term)
	go func() {
		for term := range termChan {
			// get list of documents from both inverted tables
			var titleResult, bodyResult map[string][]float32
			if v, err := inv[0].Get(ctx, term); err != nil {
				panic(err)
			} else {
				titleResult = v.(map[string][]float32)
			}

			if v, err := inv[1].Get(ctx, term); err != nil {
				panic(err)
			} else {
				bodyResult = v.(map[string][]float32)
			}

			// merge document retrieved from inverted tables, and calculate norm_tf*idf
			ret := make(map[string]Rank_term)
			for docHash, listPos := range bodyResult {
				// first entry of the listPos is norm_tf*idf
				ret[docHash] = Rank_term{
					TitleWeights: nil,
					BodyWeights : []float32{listPos[0]},
				}
			}
			
			for docHash, listPos := range titleResult {
				tempVal := ret[docHash]
				// first entry of the listPos is norm_tf*idf
				tempVal.TitleWeights = []float32{listPos[0]}
				ret[docHash] = tempVal
			}
			
			out <- ret
		}
		close(out)
	}()
	return out
}
	
func fanInDocs(docsIn ... <-chan map[string]Rank_term) <- chan map[string]Rank_term {
	var wg sync.WaitGroup
	c := make(chan map[string]Rank_term)
	out := func(docs <-chan map[string]Rank_term) {
		defer wg.Done()
		for doc := range docs {
			c <- doc
		}
	}

	wg.Add(len(docsIn))
	for _, docs := range docsIn {
		go out(docs)
	}

	// close once all the output goroutines are done
	go func() {
		wg.Wait()
		close(c)
	}()
	
	return c
}

func getMagnitudeAndPR(ctx context.Context, docs <- chan Rank_result, forw db.DB) <- chan Rank_final {
	out := make(chan Rank_final)
	go func() {
		for doc := range docs {
				
	

func GetWebpages(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	query := params["terms"]
	// TODO: whether below is necessary
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		panic(err)
	}

	log.Print("Querying terms:", query)
	queryTokenised := parser.Laundry(query)

	// convert to wordHash
	for i := 0; i < len(queryTokenised); i++ {
		queryTokenised[i] := hex.EncodeToString(md5.Sum([]byte(queryTokenised[i]))[:])
	}

	// generate common channel with inputs
	termInChan := generatePipeline(queryTokenised)

	// fan-out to get term occurence from inverted tables
	numFanOut := math.Ceil(len(queryTokenised) * 0.75)
	termOutChan := make([] <-chan map[string]Rank_term, numFanOut)
	for i := 0; i < numFanOut; i ++ {
		termOutChan[i] = getFromInverted(ctx, termInChan, inv)
	}
	
	// fan-in the result and aggregate the result based on generator model
	// docsMatched has type map[string]Rank_term
	aggregatedDocs := make(map[string]Rank_term)
	for docsMatched := range fanInDocs(termOutChan...) {
		for docHash, ranks := range docsMatched {
			val := aggregatedDocs[docHash]
			val.TitleRank = append(val.TitleRank, ranks.TitleRank)
			val.BodyRank = append(val.BodyRank, ranks.BodyRank)
		}
	}	



	// common channel for inputs of final ranking calculation
	docsInChan := generateAggrDocsPipeline(aggregatedDocs)

	// fan-out to get PR and page magnitude
	numFanOut = math.Ceil(len(aggregatedDocs)* 0.75)
	docsOutChan := make([] <- chan Rank_final, len(aggregatedDocs))
	for i := 0; i < numFanOut; i++ {
		docsOutChan[i] = getMagnitudeAndPR(ctx, docsInChan, forw)
	}
		


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
