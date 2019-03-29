package main

import (
	// "../database"
	// "context"
	"fmt"
	"golang.org/x/net/html"
	// "reflect"
	// "strconv"
	//"github.com/apsdehal/go-logger"
	// "time"
	"io/ioutil"
	"bytes"
	// "sync"
	"strings"
	// "os"
	// "io"
	// "regexp"
	// "net/url"
	// "github.com/dchest/stemmer/porter2"
)

var docsDir = "docs/"

// func Index(doc []byte, urlString string, lastModified time.Time,
// 	wgIndexer *sync.WaitGroup, mutex *sync.Mutex,
// 	inverted []database.DB_Inverted, forward []database.DB) {
//
// 	defer wgIndexer.Done()
//
// 	ctx, _ := context.WithCancel(context.TODO())
//
// 	// fmt.Println("Indexing")
// 	// Set stemmer
// 	eng := porter2.Stemmer // sample: eng.Stem("delicious")
// 	// Get Last Modified from DB
// 	URL, err := url.Parse(urlString)
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	fmt.Println(URL.String())
//
// 	//BEGIN LOCK//
// 	mutex.Lock()
// 		nextDocIDBytes, errNext := forward[4].Get(ctx, []byte("nextDocID"))
// 		if errNext != nil {
// 			panic(errNext)
// 		}
//
// 		nextDocID, err := strconv.Atoi(string(nextDocIDBytes))
// 		if err != nil {
// 			panic(err)
// 		}
// 		forward[4].Set(ctx, []byte("nextDocID"), []byte(strconv.Itoa(nextDocID + 1)))
// 	mutex.Unlock()
// 	//END LOCK//
//
// 		// Save to file
// 		if _, err := os.Stat(docsDir); os.IsNotExist(err) {
// 			os.Mkdir(docsDir, 0755)
// 		}
// 		err = ioutil.WriteFile(docsDir + strconv.Itoa(nextDocID), doc, 0644)
// 		if err != nil {
// 			panic(err)
// 		}
// 		URLBytes, errMarshal := URL.MarshalBinary()
// 		if errMarshal != nil {
// 			panic(errMarshal)
// 		}
// 		forward[4].Set(ctx, []byte(strconv.Itoa(nextDocID)), URLBytes)
// }

func main() {
	var title string
	var prevToken string
	var words []string
	var cleaned string
	sample, err := ioutil.ReadFile("./Department of Computer Science and Engineering - HKUST.html")
	if err != nil {
		panic(err)
	}
	//Tokenize document
	tokenizer := html.NewTokenizer(bytes.NewReader(sample))
	for {
		tokenType := tokenizer.Next()
		// end of file or html error
		if tokenType == html.ErrorToken {
			break
		}
		token := tokenizer.Token()
		switch tokenType {
		case html.StartTagToken:
			if token.Data == "title" {
				tokenizer.Next()
				title = strings.TrimSpace(tokenizer.Token().Data)
			}
			prevToken = token.Data
			break
		case html.TextToken:
			cleaned = strings.TrimSpace(token.Data)
			if prevToken != "script" && prevToken != "style" && cleaned != "" {
				words = append(words, cleaned)
			}
		}
	}
	fmt.Println(words[4])
	fmt.Println(title)
}
