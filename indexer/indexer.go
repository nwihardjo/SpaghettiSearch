package indexer

import (
	"../database"
	"context"
	//"fmt"
	"strconv"
	//"github.com/apsdehal/go-logger"
	"time"
	"io/ioutil"
	"sync"
//	"strings"
	"os"
	"net/url"
	"github.com/dchest/stemmer/porter2"
)

var docsDir = "docs/"

func Index(doc []byte, urlString string, lastModified time.Time,
	wgIndexer *sync.WaitGroup, mutex *sync.Mutex,
	inverted []database.DB_Inverted, forward []database.DB) {

	defer wgIndexer.Done()

	ctx, _ := context.WithCancel(context.TODO())

	// fmt.Println("Indexing")

	// Regular keyword-based indexes
	// Mapping indexes
		// Word to wordID, url to pageID
	// Inverted file indexes
		// wordID to {pageID, <word positions>}
	// Forward index
		// pageID to {keywords}
	// Page properties
		// pageID to title, URL, size, last date of mod, etc
	// Link based indexes
		// Child pageID to parent pageID

		// Get Last Modified from DB
		URL, err := url.Parse(urlString)
		if err != nil {
			panic(err)
		}
	// Set stemmer
	eng := porter2.Stemmer // sample: eng.Stem("delicious")
	if err != nil {
		panic(err)
	}
	fmt.Println(URL.String())

	mutex.Lock()
	nextDocIDBytes, errNext := forward[4].Get(ctx, []byte("nextDocID"))
	if errNext != nil {
		panic(errNext)
	}

	nextDocID, err := strconv.Atoi(string(nextDocIDBytes))
	if err != nil {
		panic(err)
	}
	forward[4].Set(ctx, []byte("nextDocID"), []byte(strconv.Itoa(nextDocID + 1)))
	mutex.Unlock()

		// Save to file
		if _, err := os.Stat(docsDir); os.IsNotExist(err) {
			os.Mkdir(docsDir, 0755)
		}
		err = ioutil.WriteFile(docsDir + strconv.Itoa(nextDocID), doc, 0644)
		if err != nil {
			panic(err)
		}
		URLBytes, errMarshal := URL.MarshalBinary()
		if errMarshal != nil {
			panic(errMarshal)
		}
		forward[4].Set(ctx, []byte(strconv.Itoa(nextDocID)), URLBytes)
}
