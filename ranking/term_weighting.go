package ranking
/*
import (
	"encoding/json"
	"math"
	"io/ioutil"
	db "the-SearchEngine/database"
	"the-SearchEngine/indexer"
	"context"
)

type tableHolder struct {
	Key	string
	Val	map[string][]uint32
}


func Compute_tfidf(ctx context.Context, forw []db.DB, inv []db.DB) error {
	var totalDocs float64
	if cachedWebpages, err := ioutil.ReadDir(indexer.DocsDir); err != nil {
		return err
	} else {
		totalDocs = float64(len(cachedWebpages))
	}

	comp, err := inv[0].Iterate(ctx)
	if err != nil {
		return 
	}
	
	// initialise storage holder for each inverted table
	KV := make([]tableHolder, len(comp.KV))
	for i := 1; i <= len(comp.KV); i++ {
		KV[i].Key = string(comp.KV[i].Key)
		if err = json.Unmarshal(comp.KV[i].Value, &(KV[i].Val)); err != nil {
			return err 
		}

		for k, v := range KV[i].Val {
		}			

		// add idf
		KV[i].Val["idf"] = math.Log2(totalDocs / float64(len(KV[i].Val)))
	}

	return nil
}
*/
