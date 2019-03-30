package main

import (
	"../database"
	"context"
	"fmt"
	"golang.org/x/net/html"
	// "reflect"
	"strconv"
	//"github.com/apsdehal/go-logger"
	"time"
	"io/ioutil"
	"bytes"
	"sync"
	"strings"
	"os"
	// "io"
	"regexp"
	"net/url"
	"github.com/surgebase/porter2"
	"github.com/dgraph-io/badger"
	"encoding/json"
	"github.com/apsdehal/go-logger"
)

var docsDir = "docs/"
var stopWords = make(map[string]bool)
func isStopWord(s string) (isStop bool) {
	// create stopWords map if its 0
	if len(stopWords) == 0 {
		// import stopword file
		content, err := ioutil.ReadFile("./stopwords.txt")
		if err != nil {
			panic(err)
		}
		wordString := strings.Split(string(content), "\n")
		for _,word := range wordString {
			stopWords[word] = true
		}
	}
	isStop = stopWords[s]
	return
}
func laundry(s string) (c []string) {
	// remove all special characters
	regex := regexp.MustCompile("[^a-zA-Z0-9]")
	s = regex.ReplaceAllString(s, " ")
	// remove unnecessary spaces
	regex = regexp.MustCompile("[^\\s]+")
	words:= regex.FindAllString(s,-1)
	// loop through each word and clean them ~laundry time~
	for _,word := range words {
		cleaned := strings.TrimSpace(strings.ToLower(word))
		cleaned = porter2.Stem(cleaned)
		if !isStopWord(cleaned) {
			c = append(c, cleaned)
		}
	}
	return
}

func getWordInfo(words []string) (termFreq map[string]int32,termPos map[string][]int32){
	termFreq = make(map[string]int32)
	termPos = make(map[string][]int32)
	for pos, word := range words {
		termPos[word] = append(termPos[word], int32(pos))
		termFreq[word] = termFreq[word]+1
	}
	return
}

func Index(doc []byte, urlString string, lastModified time.Time,
	wgIndexer *sync.WaitGroup, mutex *sync.Mutex,
	inverted []database.DB_Inverted, forward []database.DB, children []string) {

	defer wgIndexer.Done()
	var title string
	var prevToken string
	var words []string
	var cleaned string

	ctx, _ := context.WithCancel(context.TODO())

	// Get Last Modified from DB
	URL, err := url.Parse(urlString)

	if err != nil {
		panic(err)
	}

	fmt.Println(URL.String())

	//BEGIN LOCK//
	mutex.Lock()
		nextDocIDBytes, errNext := forward[4].Get(ctx, []byte("nextDocID"))
		if errNext != badger.ErrKeyNotFound {
			// masukkin 0 as nextDocID
			forward[4].Set(ctx, []byte("nextDocID"), []byte(strconv.Itoa(0)))
		} else if errNext != nil {
			panic(errNext)
		}
		nextWordIDBytes, errNext := forward[4].Get(ctx, []byte("nextWordID"))
		if errNext != badger.ErrKeyNotFound {
			// masukkin 0 as nextDocID
			forward[4].Set(ctx, []byte("nextWordID"), []byte(strconv.Itoa(0)))
		} else if errNext != nil {
			panic(errNext)
		}

		nextDocID, err := strconv.Atoi(string(nextDocIDBytes))
		if err != nil {
			panic(err)
		}
		nextWordID, err := strconv.Atoi(string(nextWordIDBytes))
		if err != nil {
			panic(err)
		}
		forward[4].Set(ctx, []byte("nextWordID"), []byte(strconv.Itoa(nextWordID + 1)))
		forward[4].Set(ctx, []byte("nextDocID"), []byte(strconv.Itoa(nextDocID + 1)))
	mutex.Unlock()
	//END LOCK//

	//Tokenize document
	tokenizer := html.NewTokenizer(bytes.NewReader(doc))
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
			if token.Data == "a" {
				tokenizer.Next()
			}
			prevToken = token.Data
			break
		case html.TextToken:
			cleaned = strings.TrimSpace(token.Data)
			if prevToken != "script" && prevToken != "style" && cleaned != "" {
				words = append(words, cleaned)
			}
			break
		}
	}
	// tokenize terms in title and body
	cleanTitle := laundry(title)
	cleanBody := laundry(strings.Join(words, " "))

	// get words info
	_, pos := getWordInfo(cleanBody)
	for _, word := range cleanTitle {
		// save from title wordID -> [{DocID, Pos}]
		// set InvKeyword_value
		invKeyVal := database.InvKeyword_value{int32(nextDocID), pos[word]}
		invKeyVals := []database.InvKeyword_value{invKeyVal,}
		mInvVal, err := json.Marshal(invKeyVal)
		if err != nil {
			panic(err)
		}
		mInvVals, err := json.Marshal(invKeyVals)
		if err != nil {
			panic(err)
		}
		hasWord, err := forward[0].Has(ctx, []byte(word))
		if err != nil {
			panic(err)
		}
		if hasWord {
			wordID, err := forward[0].Get(ctx, []byte(word))
			if err != nil {
				panic(err)
			}
			hasWordID, err := inverted[0].Has(ctx, wordID)
			if err != nil {
				panic(err)
			}
			if hasWordID{
				// append both values are byte[]
				inverted[0].AppendValue(ctx, wordID, mInvVal)
			} else {
				// insert the list of inv
				inverted[0].Set(ctx, wordID, mInvVals)
			}
		} else {
			forward[0].Set(ctx, []byte(word), []byte(strconv.Itoa(nextWordID)))
			forward[0].Set(ctx, []byte(strconv.Itoa(nextWordID)), []byte(word))
			// insert the title with the new wordID
			inverted[0].Set(ctx, []byte(strconv.Itoa(nextWordID)), mInvVals)
		}
		// save from body wordID-> [{DocID, Pos}]

		// forw[0] save word -> wordID
		// forw[1] save wordID -> word
	}

	// forw[2] save URL -> DocInfo
	// URL to the marshalling stuff
	// forw[3] save DocID -> URL
	// use the nextDocID to assign to this URL

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
	// update forward table for DocID and its corresponding URL
	forward[3].Set(ctx, []byte(strconv.Itoa(nextDocID)), URLBytes)
}

func main() {
	// var title string
	// var prevToken string
	// var words []string
	// var cleaned string
	// var wg sync.WaitGroup
	var wgIndexer sync.WaitGroup
	var mutex sync.Mutex
	ctx, cancel := context.WithCancel(context.TODO())
	log, _ := logger.New("test", 1)
	inv, forw, _ := database.DB_init(ctx, log)
	// TODO: Check nextDocID here
	for _, bdb_i := range inv {
		defer bdb_i.Close(ctx, cancel)
	}
	for _, bdb := range forw {
		defer bdb.Close(ctx, cancel)
	}
	sample, err := ioutil.ReadFile("./Department of Computer Science and Engineering - HKUST.html")
	Index(sample, "funURL", time.Now(),
		&wgIndexer, &mutex,
		inv, forw, nil)
	if err != nil {
		panic(err)
	}
	// //Tokenize document
	// tokenizer := html.NewTokenizer(bytes.NewReader(sample))
	// for {
	// 	tokenType := tokenizer.Next()
	// 	// end of file or html error
	// 	if tokenType == html.ErrorToken {
	// 		break
	// 	}
	// 	token := tokenizer.Token()
	// 	switch tokenType {
	// 	case html.StartTagToken:
	// 		if token.Data == "title" {
	// 			tokenizer.Next()
	// 			title = strings.TrimSpace(tokenizer.Token().Data)
	// 		}
	// 		if token.Data == "a" {
	// 			tokenizer.Next()
	// 		}
	// 		prevToken = token.Data
	// 		break
	// 	case html.TextToken:
	// 		cleaned = strings.TrimSpace(token.Data)
	// 		if prevToken != "script" && prevToken != "style" && cleaned != "" {
	// 			words = append(words, cleaned)
	// 		}
	// 		break
	// 	}
	// }
	// _, pos := getWordInfo(laundry(strings.Join(words, " ")))
	// try := database.InvKeyword_value{1,pos["facebook"]}
	// fmt.Println(listTry)
	// fmt.Println(laundry(title))
}
