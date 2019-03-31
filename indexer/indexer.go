package indexer

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
)

var docsDir = "docs/"
var stopWords = make(map[string]bool)
func isStopWord(s string) (isStop bool) {
	// create stopWords map if its 0
	if len(stopWords) == 0 {
		// import stopword file
		content, err := ioutil.ReadFile("./indexer/stopwords.txt")
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

func getWordInfo(words []string) (termFreq map[string]uint32,termPos map[string][]uint32){
	termFreq = make(map[string]uint32)
	termPos = make(map[string][]uint32)
	for pos, word := range words {
		termPos[word] = append(termPos[word], uint32(pos))
		termFreq[word] = termFreq[word]+1
	}
	return
}

func setInverted(ctx context.Context, word string, pos map[string][]uint32, nextDocID int, forward []database.DB, inverted database.DB_Inverted){
	// set InvKeyword_value
	invKeyVal := database.InvKeyword_value{uint16(nextDocID), pos[word]}
	mInvVal, err := json.Marshal(invKeyVal)
	if err != nil {
		panic(err)
	}
	// set InvKeyword_values
	invKeyVals := []database.InvKeyword_value{invKeyVal,}
	mInvVals, err := json.Marshal(invKeyVals)
	if err != nil {
		panic(err)
	}
	// Get wordID equivalent of current word
	wordID, err := forward[0].Get(ctx, []byte(word))
	// fmt.Println(nextWordID)
	// fmt.Println(word, wordID)
	// if there is no word to wordID mapping
	if err == badger.ErrKeyNotFound {
		// get latest wordID
			nextWordIDBytes, errNext := forward[4].Get(ctx, []byte("nextWordID"))
			if errNext == badger.ErrKeyNotFound {
				// masukkin 0 as nextWordID
				nextWordIDBytes = []byte(strconv.Itoa(0))
				forward[4].Set(ctx, []byte("nextWordID"), nextWordIDBytes)
			} else if errNext != nil {
				panic(errNext)
			}
			nextWordID, err := strconv.Atoi(string(nextWordIDBytes))
			if err != nil {
				panic(err)
			}
			// use nextWordID
			wordID = []byte(strconv.Itoa(nextWordID))
			// fmt.Println("new", newWordID)
			// forw[0] save word -> wordID
			forward[0].Set(ctx, []byte(word), wordID)
			// forw[1] save wordID -> word
			forward[1].Set(ctx, wordID, []byte(word))
			// update latest wordID
			forward[4].Set(ctx, []byte("nextWordID"), []byte(strconv.Itoa(nextWordID + 1)))
	} else if err != nil {
		panic(err)
	}
	hasWordID, err := inverted.Has(ctx, wordID)
	if err != nil {
		panic(err)
	}
	if hasWordID{
		// append both values are byte[]
		inverted.AppendValue(ctx, wordID, mInvVal)
	} else {
		// insert the list of inv
		inverted.Set(ctx, wordID, mInvVals)
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

	fmt.Println("Indexing", URL.String())

	//BEGIN LOCK//
	mutex.Lock()
	nextDocIDBytes, errNext := forward[4].Get(ctx, []byte("nextDocID"))
	if errNext == badger.ErrKeyNotFound {
		// masukkin 0 as nextDocID
		// fmt.Println("initialize next DocID")
		forward[4].Set(ctx, []byte("nextDocID"), []byte(strconv.Itoa(0)))
		nextDocIDBytes = []byte(strconv.Itoa(0))
	} else if errNext != nil {
		panic(errNext)
	}
	nextDocID, err := strconv.Atoi(string(nextDocIDBytes))
	if err != nil {
		panic(err)
	}
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
		prevToken = token.Data
		break
		case html.TextToken:
			cleaned = strings.TrimSpace(token.Data)
			if prevToken != "script" && prevToken != "a" && prevToken != "style" && cleaned != ""{
				words = append(words, cleaned)
			}
		break
		}
	}
	// tokenize terms in title and body
	cleanTitle := laundry(title)
	cleanBody := laundry(strings.Join(words, " "))
	// get words info
	_, posTitle := getWordInfo(cleanTitle)
	freqBody, posBody := getWordInfo(cleanBody)
	for _, word := range cleanTitle {
		// save from title wordID -> [{DocID, Pos}]
		setInverted(ctx, word, posTitle, nextDocID, forward, inverted[0])
	}
	for _, word := range cleanBody {
		// save from body wordID-> [{DocID, Pos}]
		setInverted(ctx, word, posBody, nextDocID, forward, inverted[1])
		// fmt.Println("HEL",word, nextWordID)
	}
	forward[4].Set(ctx, []byte("nextDocID"), []byte(strconv.Itoa(nextDocID + 1)))

	// forw[2] save URL -> DocInfo
	// URL to the marshalling stuff
	// parse title
	pageTitle := strings.Fields(title)
	pageSize := len(doc)
	URLBytes, errMarshal := URL.MarshalBinary()
	if errMarshal != nil {
	panic(errMarshal)
	}
	wordMapping := make(map[uint32]uint32)
	for word, _ := range freqBody {
		wordIDBytes, err := forward[0].Get(ctx, []byte(word))
		if err != nil {
			panic(err)
		}
		wordID, err := strconv.Atoi(string(wordIDBytes))
		if err != nil {
			fmt.Println(word)
			panic(err)
		}
		wordMapping[uint32(wordID)] = freqBody[word]
	}
	pageInfo := database.DocInfo{uint16(nextDocID), pageTitle, lastModified, uint32(pageSize), nil, nil, wordMapping}
	// marshal pageInfo
	mPageInfo, err := pageInfo.MarshalJSON()
	if err != nil {
		panic(err)
	}
	// insert into forward 2
	forward[2].Set(ctx, URLBytes, mPageInfo)

	mutex.Unlock()
	//END LOCK//
	// type DocInfo struct {
	// 	DocId         uint16            `json:"DocId"`
	// 	Page_title    []string          `json:"Page_title"`
	// 	Mod_date      time.Time         `json:"Mod_date"`
	// 	Page_size     uint32            `json:"Page_size"`
	// 	Children      []uint16          `json:"Childrens"`
	// 	Parents       []uint16          `json:"Parents"`
	// 	Words_mapping map[uint32]uint32 `json:"Words_mapping"`
	// 	//mapping for wordId to wordFrequency
	// }

	// Save to file
	if _, err := os.Stat(docsDir); os.IsNotExist(err) {
	os.Mkdir(docsDir, 0755)
	}
	err = ioutil.WriteFile(docsDir + strconv.Itoa(nextDocID), doc, 0644)
	if err != nil {
	panic(err)
	}
	// update forward table for DocID and its corresponding URL
	forward[3].Set(ctx, []byte(strconv.Itoa(nextDocID)), URLBytes)
}

// func main() {
// var title string
// var prevToken string
// var words []string
// var cleaned string
// var wg sync.WaitGroup
// var wgIndexer sync.WaitGroup
// var mutex sync.Mutex
// ctx, cancel := context.WithCancel(context.TODO())
// log, _ := logger.New("test", 1)
// inv, forw, _ := database.DB_init(ctx, log)
// // TODO: Check nextDocID here
// for _, bdb_i := range inv {
// 	defer bdb_i.Close(ctx, cancel)
// }
// for _, bdb := range forw {
// 	defer bdb.Close(ctx, cancel)
// }
// sample, err := ioutil.ReadFile("./Department of Computer Science and Engineering - HKUST.html")
// Index(sample, "funURL", time.Now(),
// 	&wgIndexer, &mutex,
// 	inv, forw, nil)
// if err != nil {
// 	panic(err)
// }
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
// }
