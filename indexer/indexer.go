package indexer

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
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

func setInverted(ctx context.Context, word string, pos map[string][]uint32, docHash string, forward []database.DB, inverted database.DB, bw_forward []database.BatchWriter, bw_inverted database.BatchWriter, mutex *sync.Mutex) {

	// initialise inverted keywords values
	invKeyVals := make(map[string][]uint32)
	invKeyVals[docHash] = pos[word]

	// Compute the wordHash of current word
	wordHash := md5.Sum([]byte(word))
	wordHashString := hex.EncodeToString(wordHash[:])

	// Check if current wordHash exist
	_, err := forward[0].Get(ctx, wordHashString)

	// If not exist, create one
	if err == badger.ErrKeyNotFound {
		// batch writer accepts array of byte only, conversion to []byte is needed
		// forw[0] save wordHash -> word
		if err = bw_forward[0].BatchSet(ctx, wordHashString, word); err != nil {
			panic(err)
		}
	} else if err != nil {
		panic(err)
	}

	// START OF CRITICAL SECTION //
	// LOCK //
	mutex.Lock()

	// append the added entry (docHash and pos) to inverted file
	// value has type of map[DocHash][]uint32 (docHash -> list of position)
	value, err := inverted.Get(ctx, wordHashString)
	if err == badger.ErrKeyNotFound {
		// there's no entry on the inverted table for the corresponding wordHash
		if err = bw_inverted.BatchSet(ctx, wordHashString, invKeyVals); err != nil {
			panic(err)
		}
	} else if err != nil {
		panic(err)
	} else {
		// append new docHash entry to the existing one
		value.(map[string][]uint32)[docHash] = append(value.(map[string][]uint32)[docHash], pos[word]...)

		// load new appended value of inverted table according to the wordHash
		if err = bw_inverted.BatchSet(ctx, wordHashString, value); err != nil {
			panic(err)
		}
	}

	// END OF CRITICAL SECTION //
	// UNLOCK //
	mutex.Unlock()

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
	inverted []database.DB, forward []database.DB,
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

	// initiate batch writer object
	var batchWriter_forward []database.BatchWriter
	var batchWriter_inverted []database.BatchWriter
	for _, i := range forward {
		temp := i.BatchWrite_init(ctx)
		defer temp.Cancel(ctx)
		batchWriter_forward = append(batchWriter_forward, temp)
	}
	for _, i := range inverted {
		temp := i.BatchWrite_init(ctx)
		defer temp.Cancel(ctx)
		batchWriter_inverted = append(batchWriter_inverted, temp)
	}
		
	// process and load data to batch writer for inverted tables
	// map word to wordHash as well if not exist
	for word, _ := range posTitle {
		// save from title wordHash -> [{DocHash, Positions}]
		setInverted(ctx, word, posTitle, docHashString, forward, inverted[0], batchWriter_forward, batchWriter_inverted[0], mutex)
	}
	for word, _ := range posBody {
		// save from body wordHash-> [{DocHash, Positions}]
		setInverted(ctx, word, posBody, docHashString, forward, inverted[1], batchWriter_forward, batchWriter_inverted[1], mutex)
	}

	// write the key-value pairs set on batch write. If no value is to be flushed, it'll return nil
	for _, f := range batchWriter_forward {
		if err = f.Flush(ctx); err != nil {
			panic(err)
		}
	}
	for _, i := range batchWriter_inverted {
		if err = i.Flush(ctx); err != nil {
			panic(err)
		}
	}

	// initialise batch writer for child append
	bw_child := forward[1].BatchWrite_init(ctx)
	defer bw_child.Cancel(ctx)

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

			// Set docHash of child -> docInfo of child using batch writer
			if err = bw_child.BatchSet(ctx, childHashString, docInfoC); err != nil {
				panic(err)
			}
		} else if err != nil {
			panic(err)
		}

		kids = append(kids, childHashString)
	}

	// Save children data into the db
	if err = bw_child.Flush(ctx); err != nil {
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
