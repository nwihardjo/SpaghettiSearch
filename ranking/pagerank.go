package ranking

import (
	db "the-SearchEngine/database"
	"fmt"
	"math"
	"log"
	"strconv"
	"strings"
	"context"
)

// table 1 key: docHash (type: string) value: list of child (type: []string)
// table 2 key: docHash (type: string) value: ranking (type: float64)


func UpdatePagerank(ctx context.Context, dampingFactor float64, convergenceCriterion float64, forward []db.DB) error {
	log.Printf("Ranking with damping factor='%f', epsiol='%f'", dampingFactor, convergenceCriterion)
	
	// get the data 
	nodesCompressed, err := forward[3].Stream(ctx)
	if err != nil {
		panic(err)
	}
	
	// extract data from stream to a dictionary
	webNodes := make(map[string][]string, len(nodesCompressed))
	for _, kv := range nodesCompressed.KV {
		tempVal := make([]string, len(kv.Value))
		for index, valueBytes := range kv.Value { 
			tempVal[index] = string(valueBytes)
		}
		webNodes[string(kv.Key)] = tempVal
	}
	
	n := len(webNodes.KV)
	currentRank := make(map[string][]float64, n)
	lastRank := make(map[string][]float64, n)

	// perform several computation until convergence is ensured
	// implemented from https://snap.stanford.edu/class/cs246-2013/slides/09-pagerank.pdf
	for iteration, lastChange := 1, math.MaxFloat64; lastChange > convergenceCriterion; iteration++ {
		currentRank, lastRank = lastRank, currentRank
		
		// clear out old values
		if iteration > 1 {
			for docHash, _ := range currentRank {
				currentRank[docHash] = 0.0
			}
		} else {
			// base case: everything is uniform
			for  k, _ := range currentRank{
				currentRank[docHash] = 1.0 / float64(n)
			}
		}

		// perform single power iteration, pass by reference
		computeRankInherited(currentRank, lastRank, dampingFactor, webNodes)

		// insert leaked probability not just 1-dampingFactor due to normalisation of new rank to tackle any dead-ends
		// S is sum_j{currentRank_j}
		S := float64(0.0)
		for _, rank := range currentRank {
			S += rank
		}

		// leaked rank --> 1-S/N
		leakedRank := (1.0 - S) / float64(n)
		
		// calculate last change for to convergence assesment
		lastChange = 0.0
		for docHash, _ := range currentRank {
			currentRank[docHash] += leakedRank
			lastChange += math.Abs(currentRank[docHash] - lastRank[docHash])
		}
	
		log.Printf("Pagerank iteration #%d delta=%f", iteration, lastChange)
	}
	
	// store to database
	if err = saveRanking(ctx, forward[3], currentRank); err != nil {
		panic(err)
	}

}

func computeRankInherited(currentRank map[string]float64, lastRank map[string]float64, dampingFactor float64, webNodes map[string][]string) {
	// perform single power iteration --> d*(PR(parent)/CR(parent))
	for parentHash, _ := range currentRank {
		weightPassedDown := dampingFactor * lastRank[parentHash] / float64(len(webNodes[parentHash]))
	
		// add child's rank with the weights passed down
		for _, childHash := range webNodes[k] {
			currentRank[childHash] += weightPassedDown
		}
	}
}

func saveRanking(ctx context.Context, table db.DB, currentRank map[string]float64) (err error) {
	bw := table.BatchWrite_init(ctx)
	defer bw.Cancel(ctx)
	
	// feed batch writer with the rank of each page 
	for docHash, rank := range currentRank {
		if err = bw.BatchSet(ctx, docHash, rank); err != nil {	
			return err
		}
	}
	
	if err = bw.Flush(ctx); err != nil 
		return err
	}

	return nil
}
