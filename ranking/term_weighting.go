package ranking

import (
	"encoding/json"
	"math"
	"io/ioutil"
	db "the-SearchEngine/database"
	"the-SearchEngine/indexer"
	"context"
)

type magnitudeValue map[string]float64

func UpdateTermWeights(ctx context.Context, inv *db.DB, forw *db.DB, info string) {
	// calculate number of webpages indexed based on saved html
	var totalDocs float64
	if cachedWebpages, err := ioutil.ReadDir(indexer.DocsDir); err != nil {
		panic(err)
	} else {
		totalDocs = float64(len(cachedWebpages))
	}

	bw := (*inv).BatchWrite_init(ctx)
	defer bw.Cancel(ctx)
	comp, err := (*inv).Iterate(ctx)
	if err != nil {
		panic(err)
	}
	
	pageMagnitude := make(map[string]float64, int(totalDocs))
	
	// iterate through each row in table to compute tf-idf
	for i := 0; i < len(comp.KV); i++ {
		// extract key-value pair from db
		key := string(comp.KV[i].Key)
		var val map[string][]float32
		if err = json.Unmarshal(comp.KV[i].Value, &val); err != nil {
			panic(err)
		}

		idf := float32(math.Log2(totalDocs / float64(len(val))))
		
		// compute tf-idf for each docs in that term
		for docHash, listPos := range val {
			// first entry of list position is normalised tf
			listPos[0] *= idf
			val[docHash] = listPos
			pageMagnitude[docHash] += float64(listPos[0] * listPos[0])
		}
		
		if err = bw.BatchSet(ctx, key, val); err != nil {
			panic(err)
		}
	}
	if err = bw.Flush(ctx); err != nil {
		panic(err)
	}

	// save page magnitude to forw[3]
	saveMagnitude(ctx, pageMagnitude, forw, info)
}

func saveMagnitude(ctx context.Context, pageMagnitude map[string]float64, forw *db.DB, info string) {
	// iterate and get all of the value from the table
	comp, err := (*forw).Iterate(ctx)
	if err != nil {
		panic(err)
	}
	
	// base case: db is empty, computing magnitude for the first time
	if len(comp.KV) == 0 {
		bw := (*forw).BatchWrite_init(ctx)
		defer bw.Cancel(ctx)
		
		for docHash, magnitude := range pageMagnitude {
			if err = bw.BatchSet(ctx, docHash, map[string]float64{info: math.Sqrt(magnitude)}); err != nil {
				panic(err)
			}
		}

		if err = bw.Flush(ctx); err != nil {
			panic(err)
		}

		return
	} else {
		// it is assumed that every webpage has body as well as title
		// container for key-value pair to be batch written
		key := make([]string, len(comp.KV))
		val := make([]map[string]float64, len(comp.KV))

		// append value for existing db
		for i := 0; i < len(comp.KV); i++ {
			key[i] = string(comp.KV[i].Key)
			var tempVal magnitudeValue
			if err = json.Unmarshal(comp.KV[i].Value, &tempVal); err != nil {
				panic(err)
			}
			
			// append provided magnitude to the value of the table
			tempVal[info] = math.Sqrt(pageMagnitude[key[i]])
			val[i] = tempVal
		}

		// batch write to db 
		bw := (*forw).BatchWrite_init(ctx)
		defer bw.Cancel(ctx)
		
		for i := 0; i < len(key); i++ {
			if err = bw.BatchSet(ctx, key[i], val[i]); err != nil {
				panic(err)
			}
		}

		if err = bw.Flush(ctx); err != nil {
			panic(err)
		}
		return
	}
}
