package indexer

import (
	"fmt"
	//"encoding/json"
	"time"
	"io/ioutil"
	"sync"
	"strings"
	"os"
)

func Index(doc []byte, url string, lastModified time.Time, wgIndexer *sync.WaitGroup) {
	defer wgIndexer.Done()

	// fmt.Println("Indexing")

	// Get Last Modified from DB

	// Save to file
	if _, err := os.Stat("docs/"); os.IsNotExist(err) {
		os.Mkdir("docs/", 0755)
	}
	err := ioutil.WriteFile("docs/" + strings.ReplaceAll(strings.ReplaceAll(url, "https://", ""), "/", "_"), doc, 0644)
	if err != nil {
		panic(err)
	}
}
