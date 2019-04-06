package indexer

import (
	"bytes"
	"context"
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

func setInverted(ctx context.Context, word string, pos map[string][]uint32, nextDocID uint16, forward []database.DB, inverted database.DB_Inverted,
	batchDB_forw []*badger.WriteBatch, batchDB_inv *badger.WriteBatch, nWID *uint32) {

	// initialise inverted keywords values
	invKeyVal := make(map[uint16][]uint32)
	invKeyVal[uint16(nextDocID)] = pos[word]

	// make inverted keywords values into []byte for batch writer
	mInvVals, err := json.Marshal(invKeyVal)
	if err != nil {
		panic(err)
	}

	var wordID uint32
	// get the word id if present in the db
	wordID_, err := forward[0].Get(ctx, word)
	if err == badger.ErrKeyNotFound {
		// use nextWordID if not found
		wordIDbyte := []byte(strconv.Itoa(int(*nWID)))
		wordID = *nWID 

		// batch writer accepts array of byte only, conversion to []byte is needed
		// forw[0] save word -> wordId
		if err = batchDB_forw[0].Set([]byte(word), wordIDbyte, 0); err != nil {
			panic(err)
		}
		// forw[1] save wordID -> word
		if err = batchDB_forw[1].Set(wordIDbyte, []byte(word), 0); err != nil {
			panic(err)
		}
		// update latest wordID
		*nWID += 1
		if err = batchDB_forw[4].Set([]byte("nextWordID"), []byte(strconv.Itoa(int(*nWID))), 0); err != nil {
			panic(err)
		}
	} else if err != nil {
		panic(err)
	} else { 
		wordID = wordID_.(uint32) 
	}

	// append the added entry (docId and pos) to inverted file
	// value has type of map[uint16][]uint32 (docId -> list of position)
	value, err := inverted.Get(ctx, wordID)
	if err == badger.ErrKeyNotFound {
		// there's no entry on the inverted table for the corresponding wordid
		if err = batchDB_inv.Set([]byte(strconv.Itoa(int(wordID))), mInvVals, 0); err != nil {
			panic(err) 
		}
	} else if err != nil {
		panic(err)
	} else {
		// append new docid entry to the existing one
		for k, v := range invKeyVal {
			value.(map[uint16][]uint32)[k] = v
		}
		
		// need to convert it back to []byte for batch writer
		tempVal, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}

		// load new appended value of inverted table according to the wordid
		if err = batchDB_inv.Set([]byte(strconv.Itoa(int(wordID))), tempVal, 0); err != nil {
			panic(err)
		}
	}

	return
}

func AddParent(currentURL string, parents []string,
	forw []database.DB, wgIndexer *sync.WaitGroup) {

	defer wgIndexer.Done()

	ctx, _ := context.WithCancel(context.TODO())

	docId, err := forw[2].Get(ctx, currentURL)
	if err != nil {
		panic(err)
	}
	tempdocinfoB, err := forw[3].Get(ctx, docId)
	if err != nil {
		panic(err)
	}
	//var temp database.DocInfo
	//err = temp.UnmarshalJSON(tempdocinfoB)
	//if err != nil {
	//	panic(err)
	//}
	tempdocinfo := tempdocinfoB.(database.DocInfo)
	for _, pURL := range parents {
		docIdP_, err := forw[2].Get(ctx, pURL)
		if err != nil {
			panic(err)
		}
		docIdP := docIdP_.(uint16)
		//docIdP, err := strconv.Atoi(string(docIdPB))
		//if err != nil {
		//	panic(err)
		//}
		tempdocinfo.Parents = append(tempdocinfo.Parents, docIdP)
	}
	//newDocInfoBytes, err := temp.MarshalJSON()
	//if err != nil {
	//	panic(err)
	//}
	err = forw[3].Set(ctx, docId, tempdocinfoB)
	if err != nil {
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

	//BEGIN LOCK//
	mutex.Lock()

	// check if nextDocID present in the database, set 0 if not
	var nextDocID uint32
	nextDocID_, errNext := forward[4].Get(ctx, "nextDocID")
	if errNext == badger.ErrKeyNotFound {
		// masukkin 0 as nextDocID
		nextDocID = 0 
		err = forward[4].Set(ctx, "nextDocID", nextDocID)
		if err != nil {
			panic(err)
		}
	} else if errNext != nil {
		panic(errNext)
	} else {
		nextDocID = nextDocID_.(uint32)
	}

	// check if current doc has an ID
	var docID uint16
	docID_, err := forward[2].Get(ctx, URL)
	if err == badger.ErrKeyNotFound {
		// set docID
		docID = uint16(nextDocID)
		// add this doc to forw[2]
		if err = forward[2].Set(ctx, URL, docID); err != nil {
			panic (err)
		}
		if err = forward[4].Set(ctx, "nextDocID", nextDocID+1); err != nil {
			panic (err)
		}
		nextDocID += 1
	} else if err != nil {
		panic(err)
	} else { 
		// when the docID is found on database
		docID = docID_.(uint16) 
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
			if prevToken != "script" && prevToken != "a" && prevToken != "style" && cleaned != "" {
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

	// initiate batch writer
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

	// check if next word id present in database, set 0 if not
	var nWID uint32
	nWID_, errNext_ := forward[4].Get(ctx, "nextWordID")
	if errNext_ == badger.ErrKeyNotFound {
		// masukkin 0 as nextWordID
		nWID = uint32(0)
		forward[4].Set(ctx, "nextWordID", nWID)
	} else if errNext_ != nil {
		panic(errNext_)
	} else { 
		nWID = nWID_.(uint32) 
	}

	// process and load data to batch writer for inverted tables
	// map word to word id as well if not exist
	for word, _ := range posTitle {
		// save from title wordID -> [{DocID, Pos}]
		setInverted(ctx, word, posTitle, docID, forward, inverted[0], batchDB_frw, batchDB_inv[0], &nWID)
	}
	for word, _ := range posBody {
		// save from body wordID-> [{DocID, Pos}]
		setInverted(ctx, word, posBody, docID, forward, inverted[1], batchDB_frw, batchDB_inv[1], &nWID)
	}	

	// write the data into database
	for _, f := range batchDB_frw {
		f.Flush()
		f.Cancel()
	}
	for _, i := range batchDB_inv {
		i.Flush()
		i.Cancel()
	}

	// initialise batch writer for children url
	var abatchDB_inv []*badger.WriteBatch
	var abatchDB_frw []*badger.WriteBatch
	for _, invPointer := range inverted {
		temp_ := invPointer.BatchWrite_init(ctx)
		abatchDB_inv = append(abatchDB_inv, temp_)
		defer temp_.Cancel()
	}
	for _, forwPointer := range forward {
		temp_ := forwPointer.BatchWrite_init(ctx)
		abatchDB_frw = append(abatchDB_frw, temp_)
		defer temp_.Cancel()
	}

	// retrieve the most current next doc id
	nextDocID_, err = forward[4].Get(ctx, "nextDocID")
	if err != nil {
		panic(err)
	}
	nextDocID = nextDocID_.(uint32)

	// get the docID of each child
	var kids []uint16
	for _, child := range children {
		// fmt.Println(child)
		childURL, err := url.Parse(child)
		if err != nil {
			panic(err)
		}
	
		// batch writer require []byte to be passed
		mChildURL, errMarshal := childURL.MarshalBinary()
		if errMarshal != nil {
			panic(errMarshal)
		}
	
		// get document id corresponding to the child, make one if not present
		var childID uint16
		childID_, err := forward[2].Get(ctx, childURL)
		if err == badger.ErrKeyNotFound {
			docInfoC := database.DocInfo{*childURL, nil, time.Now(), 0, nil, []uint16{uint16(docID)}, nil, }
			docInfoBytes, err := json.Marshal(docInfoC)
			if err != nil {
				panic(err)
			}

			// child is not inserted into URL->DocID; use writebatch instead
			nDID := strconv.Itoa(int(nextDocID))
			childID = uint16(nextDocID)
			if err = abatchDB_frw[2].Set(mChildURL, []byte(nDID), 0); err != nil {
				panic(err)
			}
			if err = abatchDB_frw[3].Set([]byte(nDID), docInfoBytes, 0); err != nil {
				panic(err)
			}

			// set childID
			// update nextDocID
			if err = abatchDB_frw[4].Set([]byte("nextDocID"), []byte(strconv.Itoa(int(nextDocID)+1)), 0); err != nil {
				panic(err)
			}
			nextDocID += 1
		} else if err != nil {
			panic(err)
		} else { 
			childID = childID_.(uint16) 
		}

		kids = append(kids, childID)
	}

	// load the child data into the db
	if err = abatchDB_frw[2].Flush(); err != nil {
		fmt.Println(err)
		panic(err)
	}
	if err = abatchDB_frw[3].Flush(); err != nil {
		fmt.Println(err)
		panic(err)
	}
	if err = abatchDB_frw[4].Flush(); err != nil {
		fmt.Println(err)
		panic(err)
	}

	forward[0].Debug_Print(ctx)
	// parse title
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

	// get the word mapping (wordId -> frequency) of each document
	wordMapping := make(map[uint32]uint32)
	i := 0
	for word, val := range freqBody {
		i += 1
		fmt.Println("DEBUG: loop through", i)
		wordID, err := forward[0].Get(ctx, word)
		if err != nil {
			fmt.Println("DEBUG:", word, "not present in db")
			panic(err)
		}
		wordMapping[wordID.(uint32)] = val
	}
	
	// initialise document object
	var pageInfo database.DocInfo
	if parentURL == "" {
		pageInfo = database.DocInfo{*URL, pageTitle, lastModified, uint32(pageSize), kids, nil, wordMapping,}
	} else {
		parentID, err := forward[2].Get(ctx, parentURL)
		if err != nil {
			panic(err)
		}
		pageInfo = database.DocInfo{*URL, pageTitle, lastModified, uint32(pageSize), kids, []uint16{parentID.(uint16)}, wordMapping,}
	}

	// insert into forward 3
	
	if err = forward[3].Set(ctx, docID, pageInfo); err != nil {
		panic(err)
	}

	mutex.Unlock()

	// Save to file
	if _, err := os.Stat(docsDir); os.IsNotExist(err) {
		os.Mkdir(docsDir, 0755)
	}
	if err = ioutil.WriteFile(docsDir+strconv.Itoa(int(docID)), doc, 0644); err != nil {
		panic(err)
	}
}
