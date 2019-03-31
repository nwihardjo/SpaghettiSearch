package indexer

import (
	"the-SearchEngine/database"
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
)

var docsDir = "docs/"

func Index(doc []byte, urlString string, lastModified time.Time,
	wgIndexer *sync.WaitGroup, mutex *sync.Mutex,
	inverted []database.DB_Inverted, forward []database.DB) {

	defer wgIndexer.Done()

	ctx, _ := context.WithCancel(context.TODO())

	// Get Last Modified from DB
	URL, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}

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
