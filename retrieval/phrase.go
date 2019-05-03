package retrieval

import (
	"context"
	"github.com/dgraph-io/badger"
	"math"
	"sync"
	db "the-SearchEngine/database"
)

func getPhraseFromInverted(ctx context.Context, phraseTokenised []string, inv []db.DB) <-chan map[string]Rank_term {
	out := make(chan map[string]Rank_term, 1)

	go func() {
		// generate common channel with inputs
		phraseInChan := genPhrasePipeline(phraseTokenised)

		// fan-out to get term occurence from inverted tables
		numFanOut := int(math.Ceil(float64(len(phraseTokenised)) * 1.0))
		termOutChan := [](<-chan map[string]Rank_term){}
		for i := 0; i < numFanOut; i++ {
			termOutChan = append(termOutChan, getPosTerm(ctx, phraseInChan, inv))
		}

		// fan-in the docs, and group the weights based on the phrase's term position 
		aggregatedResult := make(map[string](map[uint8]Rank_term)) 
		for docsMatched := range fanInDocs(termOutChan) {
			// below iterate through a map[string]Rank_term
			for docHash, ranks := range docsMatched {
				val_ := aggregatedResult[docHash]
				if val_ == nil {
					val_ = make(map[uint8]Rank_term)
				}

				// assign variable to the respective position term
				// will not append to the already existing termPos
				val := val_[ranks.TermPos]
				val.TitleWeights = ranks.TitleWeights
				val.BodyWeights = ranks.BodyWeights

				val_[ranks.TermPos] = val
				aggregatedResult[docHash] = val_
			}
		}

		// do intersection on processed term position, eliminate docs with no phrase
		out <- evalPhraseOccurrence(aggregatedResult, len(phraseTokenised))
	}()

	return out
}

func evalPhraseOccurrence(aggregatedResult map[string](map[uint8]Rank_term), lengthPhrase int) map[string]Rank_term {
	ret := make(map[string]Rank_term)

	// evaluate and return only documents containing the phrase
	// termWeights below is map[uint8]Rank_term
	for docHash, termWeights := range aggregatedResult {
		var sumBodyWeight, sumTitleWeight float32
		var bodyIntersect, titleIntersect []float32

		// length of termWeights should equal to the phraseToken
		if len(termWeights) != lengthPhrase {
			bodyIntersect, titleIntersect = nil, nil
		} else {
			// ASSUMING len(phrase) >= 1
			// int in termWeights[int] represent the term position
			if len(termWeights[0].BodyWeights) != 0 {
				sumBodyWeight += termWeights[0].BodyWeights[0]
				bodyIntersect = termWeights[0].BodyWeights[1:]
			}
			if len(termWeights[0].TitleWeights) != 0 {
				sumTitleWeight += termWeights[0].TitleWeights[0]
				titleIntersect = termWeights[0].TitleWeights[1:]
			}

			for idx := 1; idx < len(termWeights); idx++ {
				i := uint8(idx)

				if len(termWeights[i].BodyWeights) == 0 {
					bodyIntersect = nil
				} else {
					sumBodyWeight += termWeights[i].BodyWeights[0]
					bodyIntersect = intersect(bodyIntersect, termWeights[i].BodyWeights[1:])
				}

				if len(termWeights[i].TitleWeights) == 0 {
					titleIntersect = nil
				} else {
					sumTitleWeight += termWeights[i].TitleWeights[0]
					titleIntersect = intersect(titleIntersect, termWeights[i].TitleWeights[1:])
				}
			}
		}

		// append doc having phrase to final result
		if len(bodyIntersect) != 0 || len(titleIntersect) != 0 {
			val := ret[docHash]
			if len(bodyIntersect) != 0 {
				val.BodyWeights = append(val.BodyWeights, sumBodyWeight)
			}
			if len(titleIntersect) != 0 {
				val.TitleWeights = append(val.TitleWeights, sumTitleWeight)
			}
			ret[docHash] = val
		}
	}
	return ret
}

func genPhrasePipeline(listStr []string) <-chan termPhrase {
	out := make(chan termPhrase, len(listStr))
	defer close(out)
	for i := 0; i < len(listStr); i++ {
		out <- termPhrase{Term: listStr[i], Pos: uint8(i)}
	}
	return out
}

func getPosTerm(ctx context.Context, termChan <-chan termPhrase, inv []db.DB) <-chan map[string]Rank_term {
	out := make(chan map[string]Rank_term, len(termChan))
	defer close(out)
	var wg sync.WaitGroup

	for term := range termChan {
		wg.Add(1)
		go func(term termPhrase) {
			defer wg.Done()

			// get list of documents from both inverted tables
			var bodyResult map[string][]float32
			titleRes := getInvTitle(ctx, inv[0], term.Term)

			if v, err := inv[1].Get(ctx, term.Term); err != nil && err != badger.ErrKeyNotFound {
				panic(err)
			} else if v != nil {
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
					BodyWeights:  listPos,
					TermPos:      term.Pos,
				}
			}

			for docHash, listPos := range <- titleRes {
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
		}(term)
	}
	wg.Wait()
	return out
}
