package retrieval

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"github.com/dgraph-io/badger"
	"math"
	"strings"
	"sync"
	db "the-SearchEngine/database"
	"the-SearchEngine/parser"
	"log"
)

func Retrieve(query string, ctx context.Context, forw []db.DB, inv []db.DB) []Rank_combined {

	//---------------- QUERY PARSING ----------------//

	// separate the phrase into variable phrases, and exclude them from the query
	phrases := getPhrase(query)
	for _, term := range phrases {
		query = strings.Replace(query, "\""+string(term)+"\"", "", 1)
	}

	queryTokenised := parser.Laundry(strings.Join(strings.Fields(query), " "))
	phraseTokenised := parser.Laundry(strings.Join(phrases, " "))

	// convert term to docHash
	for i := 0; i < len(phraseTokenised); i++ {
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
	termOutChan := [](<-chan map[string]Rank_term){}
	for i := 0; i < numFanOut; i++ {
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

	for docHash, ranks := range <-docPhrase {
		val := aggregatedDocs[docHash]
		val.TitleWeights = append(val.TitleWeights, ranks.TitleWeights...)
		val.BodyWeights = append(val.BodyWeights, ranks.BodyWeights...)
		aggregatedDocs[docHash] = val
	}

	// common channel for inputs of final ranking calculation
	docsInChan := genAggrDocsPipeline(aggregatedDocs)

	// fan-out to calculate final rank from PR and page magnitude
	numFanOut = int(math.Ceil(float64(len(aggregatedDocs)) * 0.8))
	docsOutChan := [](<-chan Rank_combined){}
	for i := 0; i < numFanOut; i++ {
		docsOutChan = append(docsOutChan, computeFinalRank(ctx, docsInChan, forw, len(queryTokenised)+len(phraseTokenised)))
	}

	// fan-in final rank (generator pattern) and sort the result
	finalResult := make([]Rank_combined, 0, len(aggregatedDocs))
	log.Print(len(aggregatedDocs))
	for docRank := range fanInResult(docsOutChan) {
		finalResult = appendSort(finalResult, docRank)
	}
	log.Print("fin ", len(finalResult))

	if len(finalResult) > 50 {
		for i := 0; i < 50; i++ {
			finalResult[i].FinalRank /= (finalResult[0].FinalRank)
		}
		return finalResult[:50]
	} else {
		for i := 0; i < len(finalResult); i++ {
			finalResult[i].FinalRank /= (finalResult[0].FinalRank)
		}
		return finalResult
	}
}

func genTermPipeline(listStr []string) <-chan string {
	out := make(chan string, len(listStr))
	defer close(out)
	for i := 0; i < len(listStr); i++ {
		out <- listStr[i]
	}
	return out
}

func genAggrDocsPipeline(docRank map[string]Rank_term) <-chan Rank_result {
	out := make(chan Rank_result, len(docRank))
	defer close(out)
	for docHash, rank := range docRank {
		ret := Rank_result{DocHash: docHash, TitleRank: 0.0, BodyRank: 0.0}

		for i := 0; i < len(rank.TitleWeights); i++ {
			ret.TitleRank += float64(rank.TitleWeights[i])
		}

		for i := 0; i < len(rank.BodyWeights); i++ {
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
			} else if v != nil {
				bodyResult = v.(map[string][]float32)
			}

			// merge document retrieved from inverted tables
			ret := make(map[string]Rank_term)
			for docHash, listPos := range bodyResult {
				// first entry of the listPos is norm_tf*idf
				ret[docHash] = Rank_term{
					TitleWeights: nil,
					BodyWeights:  []float32{listPos[0]},
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

func fanInDocs(docsIn []<-chan map[string]Rank_term) <-chan map[string]Rank_term {
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

func fanInResult(docRankIn []<-chan Rank_combined) <-chan Rank_combined {
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
