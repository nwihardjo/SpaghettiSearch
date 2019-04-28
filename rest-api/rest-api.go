package main

import (
	"encoding/json"
	"log"
	"net/http"
	"context"
	"github.com/apsdehal/go-logger"
	"github.com/gorilla/mux"
	"the-SearchEngine/parser"
	"math"
	"regexp"
	db "the-SearchEngine/database"
	"sort"
	"sync"
	"encoding/hex"
	"github.com/dgraph-io/badger"
	"crypto/md5"
	"strings"
	"net/url"
	"time"
)

// global declaration used in db
var forw []db.DB
var inv []db.DB
var ctx context.Context

func genTermPipeline(listStr []string) <- chan string {
	out := make(chan string, len(listStr))
	for i := 0; i < len(listStr); i++ {
		out <- listStr[i]
	}
	close(out)
	return out
}

func genAggrDocsPipeline(docRank map[string]Rank_term) <- chan Rank_result {
	out := make(chan Rank_result, len(docRank))
	for docHash, rank := range docRank {
		ret := Rank_result{DocHash: docHash, TitleRank: 0.0, BodyRank: 0.0,}

		for i := 0; i < len(rank.TitleWeights); i++ {
			ret.TitleRank += float64(rank.TitleWeights[i])
		}

		for i := 0; i < len(rank.BodyWeights); i ++ {
			ret.BodyRank += float64(rank.BodyWeights[i])
		}

		out <- ret
	}
	close(out)
	return out
}

// several type for easier flow of channels
type Rank_term struct {
	TitleWeights	[]float32
	BodyWeights	[]float32
	// used only for phrase search
	TermPos		uint8
}

type Rank_result struct {
	DocHash		string
	TitleRank	float64
	BodyRank	float64
}

type Rank_combined struct {
	Url           url.URL        		   `json:"Url"`
	Page_title    []string       		   `json:"Page_title"`
	Mod_date      time.Time      		   `json:"Mod_date"`
	Page_size     uint32         		   `json:"Page_size"`
	Children      []string       		   `json:"Children"`
	Parents       map[string][]string	   `json:"Parents"`
	Words_mapping map[string]uint32 	   `json:"Words_mapping"`
	PageRank      float64			   `json:"PageRank"`
	FinalRank     float64			   `json:"FinalRank"`
}

func appendSort(data []Rank_combined, el Rank_combined) []Rank_combined {
	index := sort.Search(len(data), func(i int) bool { return data[i].FinalRank < el.FinalRank })
	data = append(data, Rank_combined{})
	copy(data[index+1:], data[index:])
	data[index] = el
	return data
}

func getFromInverted(ctx context.Context, termChan <-chan string, inv []db.DB) <-chan map[string]Rank_term {
	out := make(chan map[string]Rank_term)
	go func() {
		for term := range termChan {
			// get list of documents from both inverted tables
			var titleResult, bodyResult map[string][]float32
			if v, err := inv[0].Get(ctx, term); err != nil && err != badger.ErrKeyNotFound {
				panic(err)
			} else if v != nil {
				titleResult = v.(map[string][]float32)
			}

			if v, err := inv[1].Get(ctx, term); err != nil && err != badger.ErrKeyNotFound {
				panic(err)
			} else if v!= nil {
				bodyResult = v.(map[string][]float32)
			}

			// merge document retrieved from inverted tables
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
	
func fanInDocs(docsIn [] <-chan map[string]Rank_term) <- chan map[string]Rank_term {
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

func fanInResult(docRankIn []<-chan Rank_combined) <- chan Rank_combined {
	var wg sync.WaitGroup
	c := make(chan Rank_combined)
	out := func(docs <-chan Rank_combined) {
		defer wg.Done()
		for doc := range docs {
			c <- doc
		}
	}

	wg.Add(len(docRankIn))
	for _, docRank := range docRankIn {
		go out(docRank)
	}

	// close once all the output goroutines are done
	go func() {
		wg.Wait()
		close(c)
	}()
	
	return c
}

func computeFinalRank(ctx context.Context, docs <- chan Rank_result, forw []db.DB, queryLength int) <- chan Rank_combined {
	out := make(chan Rank_combined)
	go func() {
		for doc := range docs {
			// get doc metadata using future pattern for faster performance
			metadata := getDocInfo(ctx, doc.DocHash, &forw[1])

			// get pagerank value
			var PR float64
			if tempVal, err := forw[3].Get(ctx, doc.DocHash); err != nil {
				panic(err)
			} else {
				PR = tempVal.(float64)
			}

			// get page magnitude for cossim normalisation
			var pageMagnitude map[string]float64
			if tempVal, err := forw[4].Get(ctx, doc.DocHash); err != nil {
				panic(err)
			} else {
				pageMagnitude = tempVal.(map[string]float64)
			}
			
			// compute final rank
			queryMagnitude := math.Sqrt(float64(queryLength))
			doc.BodyRank /= (pageMagnitude["body"] * queryMagnitude)
			doc.TitleRank /= (pageMagnitude["title"] * queryMagnitude)
			
			// retrieve result from future
			docMetaData := <- metadata
			out <- resultFormat(docMetaData, PR, 0.4*PR + 0.4*doc.TitleRank + 0.2*doc.BodyRank)
		}
		close(out)
	}()	
	return out
}		
							
func resultFormat(metadata db.DocInfo, PR float64, finalRank float64) Rank_combined {
	return Rank_combined {
		Url		:	metadata.Url,
		Page_title	:	metadata.Page_title,
		Mod_date	:	metadata.Mod_date,
		Page_size	:	metadata.Page_size,
		Children	:	metadata.Children,
		Parents		:	metadata.Parents,
		Words_mapping	:	metadata.Words_mapping,
		PageRank	:	PR,
		FinalRank	:	finalRank,
	}
}

func getDocInfo(ctx context.Context, docHash string, forw *db.DB) <-chan db.DocInfo {
	out := make(chan db.DocInfo, 1)

	go func() {
		var ret db.DocInfo
		if tempVal, err := (*forw).Get(ctx, docHash); err != nil {
			panic(err)
		} else {
			ret = tempVal.(db.DocInfo)
		}

		out <- ret
	}()
	return out
}


var re = regexp.MustCompile(`".*?"`)

func getPhrase(s string) []string {
	ms := re.FindAllString(s, -1)
	ss := make([]string, len(ms))
	for i, m := range ms {
		ss[i] = m[1 : len(m)-1]
	}
	return ss
}

type termPhrase struct {
	Term	string
	Pos	uint8
}

func genPhrasePipeline(listStr []string) <- chan termPhrase {
	out := make(chan termPhrase, len(listStr))
	for i := 0; i < len(listStr); i++ {
		out <- termPhrase{Term: listStr[i], Pos: uint8(i),}
	}
	close(out)
	return out
}

func getPosTerm(ctx context.Context, termChan <-chan termPhrase, inv []db.DB) <-chan map[string]Rank_term {
	out := make(chan map[string]Rank_term)
	go func() {
		for term := range termChan {
			// get list of documents from both inverted tables
			var titleResult, bodyResult map[string][]float32
			if v, err := inv[0].Get(ctx, term.Term); err != nil && err != badger.ErrKeyNotFound {
				panic(err)
			} else if v != nil {
				titleResult = v.(map[string][]float32)
			}

			if v, err := inv[1].Get(ctx, term.Term); err != nil && err != badger.ErrKeyNotFound {
				panic(err)
			} else if v!= nil {
				bodyResult = v.(map[string][]float32)
			}

			// merge document retrieved from inverted tables
			ret := make(map[string]Rank_term)
			for docHash, listPos := range bodyResult {
				// first entry is norm_tf*idf, no need to be subtracted
				for i := 1; i < len(listPos); i++ {
					listPos[i] -= float32(term.Pos)
				}
				ret[docHash] = Rank_term{
					TitleWeights: nil,
					BodyWeights : listPos,
					TermPos    : term.Pos,
				}
			}
			
			for docHash, listPos := range titleResult {
				// first entry is norm_tf*idf, no need to be subtracted
				for i := 1; i < len(listPos); i++ {
					listPos[i] -= float32(term.Pos)
				}
				tempVal := ret[docHash]
				tempVal.TitleWeights = listPos
				tempVal.TermPos = term.Pos
				ret[docHash] = tempVal
			}
			
			out <- ret
		}
		close(out)
	}()
	return out
}

func sortFloat32(slice []float32) []float32 {
	sliceFloat64 := make([]float64, len(slice))
	for i := 0; i < len(slice); i++ {
		sliceFloat64[i] = float64(slice[i])
	}

	sort.Float64s(sliceFloat64)
	for i := 0; i < len(slice); i++ {
		slice[i] = float32(sliceFloat64[i])
	}
	return slice
}

func intersect(slice1, slice2 []float32) []float32 {
	var ret []float32

	// sort slice first based on sort library
	slice1 = sortFloat32(slice1)
	slice2 = sortFloat32(slice2)
	
	i, j := 0, 0
	for i != len(slice1) && j != len(slice2) {
		if slice1[i] == slice2[j] {
			ret = append(ret, slice1[i])
			i += 1
			j += 1
		} else if slice1[i] > slice2[j] {
			j += 1
		} else {
			i += 1
		}
	}
	return ret
}

func getPhraseFromInverted(ctx context.Context, phraseTokenised []string, inv []db.DB) <-chan map[string]Rank_term {
	out := make(chan map[string]Rank_term, 1)
	
	go func() {
		// generate common channel with inputs
		phraseInChan := genPhrasePipeline(phraseTokenised)

		// fan-out to get term occurence from inverted tables
		numFanOut := len(phraseTokenised)
		termOutChan := [] (<-chan map[string]Rank_term){}
		for i := 0; i < numFanOut; i ++ {
			termOutChan = append(termOutChan, getPosTerm(ctx, phraseInChan, inv))
		}

		// fan-in the docs, and group the weights based on the phrase's term position
		aggregatedResult := make(map[string](map[uint8]Rank_term))
		for docsMatched := range fanInDocs(termOutChan) {
			for docHash, ranks := range docsMatched {
				val_ := aggregatedResult[docHash]
				if val_ == nil {
					val_ = make(map[uint8]Rank_term)
				}

				val := val_[ranks.TermPos]
				val.TitleWeights = append(val.TitleWeights, ranks.TitleWeights...)
				val.BodyWeights = append(val.BodyWeights, ranks.BodyWeights...)
				val_[ranks.TermPos] = val
				aggregatedResult[docHash] = val_
			}
		}	
		
		ret := make(map[string]Rank_term)		

		// evaluate and return only documents containing the phrase
		for docHash, termWeights := range aggregatedResult {
			deleteBody, deleteTitle := false, false
			var sumBodyWeight, sumTitleWeight float32

			// numFanOut equals to the number of word in the phrase
			if len(termWeights) != numFanOut {
				deleteBody, deleteTitle = true, true
			} else {
				// TODO: assume the len(phase) >= 1
				sumBodyWeight, sumTitleWeight = termWeights[0].BodyWeights[0], termWeights[0].TitleWeights[0]
				bodyIntersect := termWeights[0].BodyWeights[1:]
				titleIntersect := termWeights[0].TitleWeights[1:]

				for idx := 1; idx < len(termWeights); idx++ {
					i := uint8(idx)
					sumBodyWeight += termWeights[i].BodyWeights[0]
					bodyIntersect = intersect(bodyIntersect, termWeights[i].BodyWeights[1:])

					sumTitleWeight += termWeights[i].TitleWeights[0]
					titleIntersect = intersect(titleIntersect, termWeights[i].TitleWeights[1:])
				}
				deleteBody, deleteTitle = len(bodyIntersect)==0, len(titleIntersect)==0
			}

			// append doc having phrase to final result
			if !deleteBody && !deleteTitle {
				val := ret[docHash]
				if !deleteBody {
					val.BodyWeights = append(val.BodyWeights, sumBodyWeight)
				}
				if !deleteTitle {
					val.TitleWeights = append(val.TitleWeights, sumTitleWeight)
				}
				ret[docHash] = val
			}
		}

		out <- ret
	}()

	return out
}

			

func GetWebpages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")


	//---------------- QUERY PARSING ----------------//

	params := mux.Vars(r)
	query := params["terms"]
	// if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
	// 	panic(err)
	// }

	query = strings.Replace(query, "-", " ", -1)	
	log.Print("Querying terms:", query)
	timer := time.Now()
	// separate the phrase into variable phrases, and exclude them from the query
	phrases := getPhrase(query)
	for _, term := range phrases {
		query = strings.Replace(query, "\""+string(term)+"\"", "", 1)
	}
	queryTokenised := parser.Laundry(strings.Join(strings.Fields(query), " "))
	phraseTokenised := parser.Laundry(strings.Join(phrases, " "))

	// convert term to docHash
	for i := 0; i < len(phrases); i++ {
		tempHash := md5.Sum([]byte(phraseTokenised[i]))
		phraseTokenised[i] = hex.EncodeToString(tempHash[:])
	}
	for i := 0; i < len(queryTokenised); i++ {
		tempHash := md5.Sum([]byte(queryTokenised[i]))
		queryTokenised[i] = hex.EncodeToString(tempHash[:])
	}


	//---------------- PHRASE RETRIEVAL ----------------//
	
	// use future pattern
	// TODO: double-check what laundry will return if "" is passed
	docPhrase := getPhraseFromInverted(ctx, phraseTokenised, inv)


	//---------------- NON-PHRASE TERM RETRIEVAL ----------------//

	// generate common channel with inputs
	termInChan := genTermPipeline(queryTokenised)

	// fan-out to get term occurence from inverted tables
	numFanOut := int(math.Ceil(float64(len(queryTokenised)) * 0.75))
	termOutChan := [] (<-chan map[string]Rank_term){}
	for i := 0; i < numFanOut; i ++ {
		termOutChan = append(termOutChan, getFromInverted(ctx, termInChan, inv))
	}
	
	// fan-in the result and aggregate the result based on generator model
	// docsMatched has type map[string]Rank_term
	aggregatedDocs := make(map[string]Rank_term)
	for docsMatched := range fanInDocs(termOutChan) {
		for docHash, ranks := range docsMatched {
			val := aggregatedDocs[docHash]
			val.TitleWeights = append(val.TitleWeights, ranks.TitleWeights...)
			val.BodyWeights = append(val.BodyWeights, ranks.BodyWeights...)
			aggregatedDocs[docHash] = val
		}
	}	

	
	//---------------- COMBINED RETRIEVAL, FINAL RANK CALCULATION ----------------//
	
	for docHash, ranks := range <- docPhrase {
		val := aggregatedDocs[docHash]
		val.TitleWeights = append(val.TitleWeights, ranks.TitleWeights...)
		val.BodyWeights = append(val.BodyWeights, ranks.BodyWeights...)
		aggregatedDocs[docHash] = val
	}

	// common channel for inputs of final ranking calculation
	docsInChan := genAggrDocsPipeline(aggregatedDocs)

	// fan-out to calculate final rank from PR and page magnitude
	numFanOut = int(math.Ceil(float64(len(aggregatedDocs))* 0.75))
	docsOutChan := [] (<-chan Rank_combined){}
	for i := 0; i < numFanOut; i++ {
		docsOutChan = append(docsOutChan, computeFinalRank(ctx, docsInChan, forw, len(queryTokenised)))
	}

	// fan-in final rank (generator pattern) and sort the result
	finalResult := make([]Rank_combined, len(aggregatedDocs))
	for docRank := range fanInResult(docsOutChan) {
		finalResult = appendSort(finalResult, docRank)
	}

	// return only top-50 document
	if len(finalResult) > 50 {
		json.NewEncoder(w).Encode(finalResult[:50])
	} else {
		json.NewEncoder(w).Encode(finalResult)
	}
	log.Print("Query processed in ", time.Since(timer))
}

func main() {
	// initialise db connection
	ctx, cancel := context.WithCancel(context.TODO())
	log_, _ := logger.New("test", 1)
	var err error
	inv, forw, err= db.DB_init(ctx, log_)
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
	log.Print("Server is running")
	router.HandleFunc("/query/{terms}", GetWebpages).Methods("GET")
	router.HandleFunc("/wordlist/{pre}", GetWordList).Methods("GET")
	log.Fatal(http.ListenAndServe(":8080", router))
}

func GetWordList(w http.ResponseWriter, r *http.Request) {
	log.Print("Getting word list...")

	pre := mux.Vars(r)["pre"]

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")

	tempT, err := inv[0].IterateInv(ctx, pre, forw[0])
	if err != nil {
		panic(err)
	}
	tempB, err := inv[1].IterateInv(ctx, pre, forw[0])
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
