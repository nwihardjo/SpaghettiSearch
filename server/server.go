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
	db "the-SearchEngine/database"
	"sort"
	"sync"
	"encoding/hex"
	"github.com/dgraph-io/badger"
	"crypto/md5"
	"strings"
	"time"
)

// global declaration used in db
var forw []db.DB
var inv []db.DB
var ctx context.Context

func genTermPipeline(listStr []string) <- chan string {
	out := make(chan string, len(listStr))
	defer close(out)
	for i := 0; i < len(listStr); i++ {
		out <- listStr[i]
	}
	return out
}

func genAggrDocsPipeline(docRank map[string]Rank_term) <- chan Rank_result {
	out := make(chan Rank_result, len(docRank))
	defer close(out)
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
	return out
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
			metadata := getDocInfo(ctx, doc.DocHash, forw)
			
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
			
			// retrieve result from future, assign ranking
			docMetaData := <- metadata
			docMetaData.PageRank = PR
			docMetaData.FinalRank = 0.4*PR + 0.4*doc.TitleRank + 0.2*doc.BodyRank

			out <- docMetaData
		}
		close(out)
	}()	
	return out
}		

func getDocInfo(ctx context.Context, docHash string, forw []db.DB) <-chan Rank_combined {
	out := make(chan Rank_combined, 1)

	go func() {
		var val db.DocInfo
		if tempVal, err := forw[1].Get(ctx, docHash); err != nil {
			panic(err)
		} else {
			val = tempVal.(db.DocInfo)
		}

		ret := resultFormat(val, 0, 0)
		
		parentChan := convertHashDocinfo(ctx, ret.Parents, forw)
		childrenChan := convertHashDocinfo(ctx, ret.Children, forw)
		wordmapChan := convertHashWords(ctx, ret.Words_mapping, forw)

		ret.Parents = <-parentChan
		ret.Children = <-childrenChan
		ret.Words_mapping = <- wordmapChan

		out <- ret
	}()
	return out
}

func convertHashDocinfo(ctx context.Context, docHashes []string, forw []db.DB) <-chan []string{
	out := make(chan []string, 1)

	// early stopping
	if docHashes == nil || len(docHashes) == 0{
		out <- nil
		return out
	}	

	go func() {
		// generate common input
		docHashInChan := genTermPipeline(docHashes)
		
		// fan-out to several getter
		numFanOut := len(docHashes)
		docOutChan := [](<-chan string){}
		for i := 0; i < numFanOut; i++ {
			docOutChan = append(docOutChan, retrieveUrl(ctx, docHashInChan, forw))
		}

		// fan-in result
		resultUrl := make([]string, len(docHashes))
		for docUrl := range fanInUrl(docOutChan) {
			resultUrl = append(resultUrl, docUrl)
		}

		out <- resultUrl
	}()
	return out
}

func retrieveUrl(ctx context.Context, docHashIn <-chan string, forw []db.DB) <-chan string {
	out := make(chan string, 1)
	go func() {
		for docHash := range docHashIn {
			var url string
			if val, err := forw[1].Get(ctx, docHash); err != nil {
				log.Print("DEBUG DOCHASH", docHash)
				panic(err)
			} else {
				doc := val.(db.DocInfo)
				url = doc.Url.String()
			}

			out <- url
		}
		close(out)
	}()
	
	return out
}

func fanInUrl(urlIn [] <-chan string) <-chan string {
	var wg sync.WaitGroup
	c := make(chan string)
	out := func(urls <-chan string) {
		defer wg.Done()
		for url := range urls {
			c <- url
		}
	}
	
	wg.Add(len(urlIn))
	for _, url := range urlIn {
		go out(url)
	}
	
	// close once all output goroutines are done
	go func() {
		wg.Wait()
		close(c)
	}()

	return c
}

func genWordPipeline(wordMap map[string]uint32) <-chan string {
	out := make(chan string, len(wordMap))
	defer close(out)
	for wordHash, _ := range wordMap {
		out <- wordHash
	}
	return out
}

func fanInWords(wordIn [] <-chan map[string]string) <-chan map[string]string {
	var wg sync.WaitGroup
	c := make(chan map[string]string)
	out := func(words <-chan map[string]string) {
		defer wg.Done()
		for mapping := range words {
			for wordHash, wordStr := range mapping {
				c <- map[string]string{wordHash: wordStr}
			}
		}
	}

	wg.Add(len(wordIn))
	for _, word := range wordIn {
		go out(word)
	}

	// close once all output goroutines are done
	go func() {
		wg.Wait()
		close(c)
	}()
	
	return c
}

func retrieveWord(ctx context.Context, wordInChan <-chan string, forw []db.DB) <-chan map[string]string {
	out := make(chan map[string]string, 1)
	go func() {
		for word := range wordInChan {
			var wordStr string
			if val, err := forw[0].Get(ctx, word); err != nil {
				panic(err)
			} else {
				wordStr = val.(string)
			}

			out <- map[string]string{word: wordStr}
		}
		close(out)
	}()
	
	return out
}

func convertHashWords(ctx context.Context, wordMap map[string]uint32, forw []db.DB) <-chan map[string]uint32{
	out := make(chan map[string]uint32, 1)

	// early stopping	
	if wordMap == nil || len(wordMap) == 0 {
		out <- nil
		return out
	}

	go func() {
		// generate common channel for word input
		wordInChan := genWordPipeline(wordMap)

		// fan-out to multiple workers to get the word in string
		// word list is limited to 5
		numFanOut := len(wordMap)
		wordOutChan := [] (<-chan map[string]string){}
		for i := 0; i < numFanOut; i ++ {
			wordOutChan = append(wordOutChan, retrieveWord(ctx, wordInChan, forw))
		}

		// fan-in word hash mapping
		ret := make(map[string]uint32, len(wordMap))
		for hashToWord := range fanInWords(wordOutChan) {
			for wordHash, wordStr := range hashToWord {
				ret[wordStr] = wordMap[wordHash]
			}
		}
		
		out <- ret
		close(out)
	}()
	return out
}

func genPhrasePipeline(listStr []string) <- chan termPhrase {
	out := make(chan termPhrase, len(listStr))
	defer close(out)
	for i := 0; i < len(listStr); i++ {
		out <- termPhrase{Term: listStr[i], Pos: uint8(i),}
	}
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

func getPhraseFromInverted(ctx context.Context, phraseTokenised []string, inv []db.DB) <-chan map[string]Rank_term {
	out := make(chan map[string]Rank_term, 1)
	
	go func() {
		// generate common channel with inputs
		phraseInChan := genPhrasePipeline(phraseTokenised)

		// fan-out to get term occurence from inverted tables
		numFanOut := int(math.Ceil(float64(len(phraseTokenised)) * 0.75))
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
	numFanOut := int(math.Ceil(float64(len(queryTokenised))* 0.75))
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
