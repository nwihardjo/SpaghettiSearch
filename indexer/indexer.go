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
)

var docsDir = "docs/"

func AddParent(currentURL string, parents []string,
	forw []database.DB, wgIndexer *sync.WaitGroup) {

	defer wgIndexer.Done()

	ctx, _ := context.WithCancel(context.TODO())

	docIdBytes, err := forw[2].Get(ctx, []byte(currentURL))
	if err != nil {
		panic(err)
	}
	tempdocinfoB, err := forw[3].Get(ctx, docIdBytes)
	if err != nil {
		panic(err)
	}
	var temp DocInfo
	err = temp.UnmarshalJSON(tempdocinfoB)
	if err != nil {
		panic(err)
	}
	for _, pURL := range parents {
		docIdPB, err := forw[2].Get(ctx, []byte(pURL))
		if err != nil {
			panic(err)
		}
		temp.Parents = append(temp.Parents, strconv.Atoi(string(docIdPB)))
	}
	newDocInfoBytes, err := temp.MarshalJSON()
	if err != nil {
		panic(err)
	}
	err = forw[3].Set(ctx, docIdBytes, newDocInfoBytes)
	if err != nil {
		panic(err)
	}

}

func Index(doc []byte, urlString string,
	lastModified time.Time, ps string, mutex *sync.Mutex,
	inverted []database.DB_Inverted, forward []database.DB,
	parentURL string, children []string) {

	defer wgIndexer.Done()

	/* parentURL == "" means nil
	if parentURL == "" {
		handle parentURL as nil
	}
	*/

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
