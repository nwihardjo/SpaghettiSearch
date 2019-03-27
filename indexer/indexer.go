package indexer

import (
	"fmt"
	//"encoding/json"
	"time"
	"io/ioutil"
	"sync"
	"strings"
	"os"
	"net/url"
)

var docsDir = "docs/"

func Index(doc []byte, urlString string, lastModified time.Time, wgIndexer *sync.WaitGroup) {
	defer wgIndexer.Done()

	// fmt.Println("Indexing")

	// Get Last Modified from DB
	URL, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	fmt.Println(URL.String())

	// Save to file
	if _, err := os.Stat(docsDir); os.IsNotExist(err) {
		os.Mkdir(docsDir, 0755)
	}
	err = ioutil.WriteFile(docsDir + strings.ReplaceAll(strings.ReplaceAll(urlString, "https://", ""), "/", "_"), doc, 0644)
	if err != nil {
		panic(err)
	}
}
