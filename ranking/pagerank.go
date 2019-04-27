package ranking

import (
	db "the-SearchEngine/database"
	"math"
	"encoding/json"
	"log"
	"context"
)

// table 1 key: docHash (type: string) value: list of child (type: []string)
// table 2 key: docHash (type: string) value: ranking (type: float64)

func UpdatePagerank(ctx context.Context, dampingFactor float64, convergenceCriterion float64, forward []db.DB) {
	log.Printf("Ranking with damping factor='%f', convergence_criteria='%f'", dampingFactor, convergenceCriterion)
	
	// get the data 
	nodesCompressed, err := forward[2].Iterate(ctx)
	if err != nil {
		panic(err)
	}
	
	// extract the data from stream
	webNodesAll := make(map[string]struct{})
	webNodes := make(map[string][]string, len(nodesCompressed.KV))
	for _, kv := range nodesCompressed.KV {
		var tempVal []string
		if err = json.Unmarshal(kv.Value, &tempVal); err != nil {
			panic(err)
		}

		// add childhash to list of webnodes
		for _, childHash := range tempVal { 
			webNodesAll[childHash] = struct{}{}
		}

		webNodes[string(kv.Key)] = tempVal
		webNodesAll[string(kv.Key)] = struct{}{}
	}

	setWebNodes := make([]string, 0, len(webNodesAll))
	for k, _ := range webNodesAll{
		setWebNodes = append(setWebNodes, k)
	}
	
	// use number of web nodes for more efficient memory allocation
	n := len(setWebNodes)
	log.Printf("number of webpages indexed %d", n)
	currentRank := make(map[string]float64, n)
	lastRank := make(map[string]float64, n)

	teleportProbs := 1.0 - dampingFactor

	// perform several computation until convergence is ensured
	for iteration, lastChange := 1, math.MaxFloat64; lastChange > convergenceCriterion; iteration++ {
		currentRank, lastRank = lastRank, currentRank
		
		// clear out old values
		if iteration > 1 {
			for _, docHash := range setWebNodes {
				currentRank[docHash] = 0.0
			}
		} else {
			// base case: everything is uniform
			if iteration == 1 {
				for _, docHash := range setWebNodes {
					lastRank[docHash] = 1.0 / float64(n)
				}
			}

			for _, docHash := range setWebNodes {
				currentRank[docHash] = 1.0 / float64(n)
			}
		}

		// perform single power iteration, pass by reference. Get totalValue for normalisation
		totalValue := computeRankInherited(currentRank, lastRank, dampingFactor, webNodes)
		totalValue += (teleportProbs * float64(len(currentRank)))

		// calculate last change for to convergence assesment based on L1 norm
		lastChange = 0.0
		for docHash, rank := range currentRank {
			currentRank[docHash] = (rank + teleportProbs) / totalValue
			lastChange += math.Abs(currentRank[docHash] - lastRank[docHash])
		}
	
		log.Printf("Pagerank iteration #%d delta=%f", iteration, lastChange)
	}
	// store to database
	if err = saveRanking(ctx, forward[3], currentRank); err != nil {
		panic(err)
	}
	return
}

func computeRankInherited(currentRank map[string]float64, lastRank map[string]float64, dampingFactor float64, webNodes map[string][]string) float64{
	totalValue := 0.0

	// perform single power iteration --> d*(PR(parent)/CR(parent))
	for parentHash, _ := range currentRank {
		// web with no child
		if len(webNodes[parentHash]) == 0 {
			continue
		}
		
		weightPassedDown := dampingFactor * lastRank[parentHash] / float64(len(webNodes[parentHash]))
		totalValue += weightPassedDown

		// add child's rank with the weights passed down
		for _, childHash := range webNodes[parentHash] {
			currentRank[childHash] += weightPassedDown
		}
	}
	return totalValue
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
	
	return bw.Flush(ctx)
}
