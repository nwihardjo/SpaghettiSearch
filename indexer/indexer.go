package indexer

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/surgebase/porter2"
	"golang.org/x/net/html"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"the-SearchEngine/database"
	"time"
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
		for _, word := range wordString {
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
	words := regex.FindAllString(s, -1)
	// loop through each word and clean them ~laundry time~
	for _, word := range words {
		cleaned := strings.TrimSpace(strings.ToLower(word))
		cleaned = porter2.Stem(cleaned)
		if !isStopWord(cleaned) {
			c = append(c, cleaned)
		}
	}
	return
}

func getWordInfo(words []string) (termFreq map[string]uint32, termPos map[string][]uint32) {
	termFreq = make(map[string]uint32)
	termPos = make(map[string][]uint32)
	for pos, word := range words {
		termPos[word] = append(termPos[word], uint32(pos))
		termFreq[word] = termFreq[word] + 1
	}
	return
}

func setInverted(ctx context.Context, word string, pos map[string][]uint32, docHash string, forward []database.DB, inverted database.DB_Inverted,
	batchDB_forw []*badger.WriteBatch, batchDB_inv *badger.WriteBatch, mutex *sync.Mutex) {

	// initialise inverted keywords values
	invKeyVals := make(map[string][]uint32)
	invKeyVals[docHash] = pos[word]

	// make inverted keywords values into []byte for batch writer
	mInvVals, err := json.Marshal(invKeyVals)
	if err != nil {
		panic(err)
	}

	// Compute the wordHash of current word
	wordHash := md5.Sum([]byte(word))
	wordHashString := hex.EncodeToString(wordHash[:])

	// Check if current wordHash exist
	_, err = forward[0].Get(ctx, wordHashString)

	// If not exist, create one
	if err == badger.ErrKeyNotFound {
		// batch writer accepts array of byte only, conversion to []byte is needed
		// forw[1] save wordHash -> word
		if err = batchDB_forw[0].Set([]byte(wordHashString), []byte(word), 0); err != nil {
			panic(err)
		}
	} else if err != nil {
		panic(err)
	}

	// append the added entry (docHash and pos) to inverted file
	// value has type of map[DocHash][]uint32 (docHash -> list of position)
	value, err := inverted.Get(ctx, wordHashString)
	if err == badger.ErrKeyNotFound {
		// there's no entry on the inverted table for the corresponding wordHash
		if err = batchDB_inv.Set([]byte(wordHashString), mInvVals, 0); err != nil {
			panic(err)
		}
	} else if err != nil {
		panic(err)
	} else {
		// append new docHash entry to the existing one
		value.(map[string][]uint32)[docHash] = append(value.(map[string][]uint32)[docHash], pos[word]...)

		// need to convert it back to []byte for batch writer
		tempVal, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}

		// load new appended value of inverted table according to the wordHash
		if err = batchDB_inv.Set([]byte(wordHashString), tempVal, 0); err != nil {
			panic(err)
		}
	}

	return
}

func AddParent(currentURL_ string, parents []string,
	forw []database.DB, wgIndexer *sync.WaitGroup) {

	defer wgIndexer.Done()
	ctx, _ := context.WithCancel(context.TODO())

	// get existing docInfo corresponding to the current docHash
	var tempdocinfo database.DocInfo
	docHash := md5.Sum([]byte(currentURL_))
	docHashString := hex.EncodeToString(docHash[:])
	tempdocinfoB, err := forw[1].Get(ctx, docHashString)
	if err != nil {
		panic(err)
	}
	tempdocinfo = tempdocinfoB.(database.DocInfo)

	// append the parents to the docInfo
	for _, pURL := range parents {
		pHash := md5.Sum([]byte(pURL))
		pHashString := hex.EncodeToString(pHash[:])
		tempdocinfo.Parents = append(tempdocinfo.Parents, pHashString)
	}

	// add back the docInfo with appended parents
	if err = forw[1].Set(ctx, docHashString, tempdocinfo); err != nil {
		panic(err)
	}

}

func Index(doc []byte, urlString string,
	lastModified time.Time, ps string, mutex *sync.Mutex,
	inverted []database.DB_Inverted, forward []database.DB,
	parentURL string, children []string) {

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

	// Get the hash of current URL
	docHash := md5.Sum([]byte(urlString))
	docHashString := hex.EncodeToString(docHash[:])

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
			if prevToken != "script" && prevToken != "a" && prevToken != "style" && cleaned != "" {
				words = append(words, cleaned)
			}
			break
		}
	}

	// Clean terms in title and body
	cleanTitle := laundry(title)
	cleanBody := laundry(strings.Join(words, " "))

	// Get frequency and positions of each term
	// in title and body
	_, posTitle := getWordInfo(cleanTitle)
	freqBody, posBody := getWordInfo(cleanBody)

	// Initialize batch writer
	var batchDB_inv []*badger.WriteBatch
	var batchDB_frw []*badger.WriteBatch
	for _, invPointer := range inverted {
		temp_ := invPointer.BatchWrite_init(ctx)
		batchDB_inv = append(batchDB_inv, temp_)
		defer temp_.Cancel()
	}
	for _, forwPointer := range forward {
		temp_ := forwPointer.BatchWrite_init(ctx)
		batchDB_frw = append(batchDB_frw, temp_)
		defer temp_.Cancel()
	}

	// START OF CRITICAL SECTION //
	// LOCK //
	mutex.Lock()

	// process and load data to batch writer for inverted tables
	// map word to wordHash as well if not exist
	for word, _ := range posTitle {
		// save from title wordHash -> [{DocHash, Positions}]
		setInverted(ctx, word, posTitle, docHashString, forward, inverted[0], batchDB_frw, batchDB_inv[0], mutex)
	}
	for word, _ := range posBody {
		// save from body wordHash-> [{DocHash, Positions}]
		setInverted(ctx, word, posBody, docHashString, forward, inverted[1], batchDB_frw, batchDB_inv[1], mutex)
	}

	// END OF CRITICAL SECTION //
	// UNLOCK //
	mutex.Unlock()

	// write the data into database
	for _, f := range batchDB_frw {
		f.Flush()
	}
	for _, i := range batchDB_inv {
		i.Flush()
	}

	// initialize batch writer for children docInfos
	abatchDB_frw := forward[1].BatchWrite_init(ctx)
	defer abatchDB_frw.Cancel()

	// Initialize container for docHashes of children
	var kids []string

	for _, child := range children {
		// Get URL object of current child url
		childURL, err := url.Parse(child)

		// Get docHash of each child
		childHash := md5.Sum([]byte(child))
		childHashString := hex.EncodeToString(childHash[:])

		// Get DocInfo corresponding to the child,
		// make one if not present (for the sake of getting the url of not-yet-visited child)
		docInfoC, err := forward[1].Get(ctx, childHashString)
		if err == badger.ErrKeyNotFound {
			docInfoC = database.DocInfo{*childURL, nil, time.Now(), 0, nil, []string{childHashString}, nil}
			docInfoBytes, err := json.Marshal(docInfoC)
			if err != nil {
				panic(err)
			}

			// Set docHash of child -> docInfo of child using batch writer
			if err = abatchDB_frw.Set([]byte(childHashString), docInfoBytes, 0); err != nil {
				panic(err)
			}
		} else if err != nil {
			panic(err)
		}

		kids = append(kids, childHashString)
	}

	// Save children data into the db
	if err = abatchDB_frw.Flush(); err != nil {
		panic(err)
	}

	// Parse title & page size
	pageTitle := strings.Fields(title)
	var pageSize int
	if ps == "" {
		pageSize = len(doc)
	} else {
		pageSize, err = strconv.Atoi(ps)
		if err != nil {
			panic(err)
		}
	}

	// Get the word mapping (wordHash -> frequency) of each document
	wordMapping := make(map[string]uint32)
	for word, val := range freqBody {
		h := md5.Sum([]byte(word))
		wordMapping[hex.EncodeToString(h[:])] = val
	}

	// Initialize document object
	var pageInfo database.DocInfo
	if parentURL == "" {
		pageInfo = database.DocInfo{*URL, pageTitle, lastModified, uint32(pageSize), kids, nil, wordMapping}
	} else {
		pHash := md5.Sum([]byte(parentURL))
		pHashString := hex.EncodeToString(pHash[:])
		pageInfo = database.DocInfo{*URL, pageTitle, lastModified, uint32(pageSize), kids, []string{pHashString}, wordMapping}
	}

	// Save docHash -> docInfo of current doc
	if err = forward[1].Set(ctx, docHashString, pageInfo); err != nil {
		panic(err)
	}

	// Save to file
	if _, err := os.Stat(docsDir); os.IsNotExist(err) {
		os.Mkdir(docsDir, 0755)
	}
	if err = ioutil.WriteFile(docsDir+docHashString, doc, 0644); err != nil {
		panic(err)
	}
}
