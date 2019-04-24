package ranking

import (
	"encoding/json"
	"math"
	"io/ioutil"
	db "the-SearchEngine/database"
	"the-SearchEngine/indexer"
	"context"
)

func Compute_idf(ctx context.Context, inv db.DB) {
	// calculate number of webpages indexed based on saved html
	var totalDocs float64
	if cachedWebpages, err := ioutil.ReadDir(indexer.DocsDir); err != nil {
		panic(err)
	} else {
		totalDocs = float64(len(cachedWebpages))
	}

	bw := inv.BatchWrite_init(ctx)
	defer bw.Cancel(ctx)

	comp, err := inv.Iterate(ctx)
	if err != nil {
		panic(err)
	}
	
	// for each item in the database, compute idf based on totalDocs and number of docs
	for i := 0; i < len(comp.KV); i++ {
		key := string(comp.KV[i].Key)
		var val map[string][]float32
		if err = json.Unmarshal(comp.KV[i].Value, &val); err != nil {
			panic(err)
		}

		val["idf"] = []float32{float32(math.Log2(totalDocs / float64(len(val))))}
		
		if err = bw.BatchSet(ctx, key, val); err != nil {
			panic(err)
		}
	}
	if err = bw.Flush(ctx); err != nil {
		panic(err)
	}

	return
}
