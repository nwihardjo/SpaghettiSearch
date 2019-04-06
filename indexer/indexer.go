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

	// set InvKeyword_value
	invKeyVal := make(map[uint16][]uint32)
	invKeyVal[uint16(nextDocID)] = pos[word]
	//invKeyVal := database.InvKeyword_value{uint16(nextDocID), pos[word]}
	mInvVals, err := json.Marshal(invKeyVal)
	if err != nil {
		panic(err)
	}
	// set InvKeyword_values
	// invKeyVals := []database.InvKeyword_value{invKeyVal}
	// mInvVals, err := json.Marshal(invKeyVals)
	// if err != nil {
	// 	panic(err)
	// }
	// Get wordID equivalent of current word

	// wordID is type uint32
	var wordID uint32
	wordID_, err := forward[0].Get(ctx, word)
	// fmt.Println(nextWordID)
	// if there is no word to wordID mapping
	if err == badger.ErrKeyNotFound {
		// use nextWordID
		wordIDbyte := []byte(strconv.Itoa(int(*nWID)))
		wordID = *nWID 
		// fmt.Println("new", newWordID)
		// forw[0] save word -> wordID
		batchDB_forw[0].Set([]byte(word), wordIDbyte, 0)
		// forw[1] save wordID -> word
		batchDB_forw[1].Set(wordIDbyte, []byte(word), 0)
		// update latest wordID
		batchDB_forw[4].Set([]byte("nextWordID"), []byte(strconv.Itoa(int(*nWID)+1)), 0)
		*nWID += 1
	} else if err != nil {
		panic(err)
	} else { wordID = wordID_.(uint32) }
	// fmt.Println(word, wordID)
	hasWordID, err := inverted.Has(ctx, wordID)
	if err != nil {
		panic(err)
	}
	if hasWordID {
		// append both values are byte[]
		//inverted.AppendValue(ctx, wordID, mInvVal)
		
		// value has type of map[uint16][]uint32
		value, err := inverted.Get(ctx, wordID)
		if err != nil {
			panic(err)
		}
	
		// var appendedValue_struct database.InvKeyword_value
		//var tempValues database.InvKeyword_values
		//err = json.Unmarshal(value, &tempValues)
		//if err != nil {
		//	panic(err)
		//}
		//err = json.Unmarshal(mInvVal, &appendedValue_struct)
		//if err != nil {
		//	panic(err)
		//}

		for k, v := range invKeyVal {
			value.(map[uint16][]uint32)[k] = v
		}
		
		//tempValues = append(tempValues, appendedValue_struct)
		tempVal, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}

		// delete and set the new appended values
		// TODO: optimise the operation
		// if err = bdb_i.Delete(ctx, key); err != nil {
		// 	return err
		// }
		if err = batchDB_inv.Set([]byte(strconv.Itoa(int(wordID))), tempVal, 0); err != nil {
			panic(err)
		}
	} else {
		// insert the list of inv
		//inverted.Set(ctx, wordID, mInvVals)
		batchDB_inv.Set([]byte(strconv.Itoa(int(wordID))), mInvVals, 0) 
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
	//URLBytes, errMarshal := URL.MarshalBinary()
	//if errMarshal != nil {
	//	panic(errMarshal)
	//}
	fmt.Println("Indexing", URL.String())

	//BEGIN LOCK//
	mutex.Lock()
	var nextDocID uint32
	nextDocID_, errNext := forward[4].Get(ctx, "nextDocID")
	if errNext == badger.ErrKeyNotFound {
		// masukkin 0 as nextDocID
		// fmt.Println("initialize next DocID")
		nextDocID = 0 
		err = forward[4].Set(ctx, "nextDocID", nextDocID)
		if err != nil {
			panic(err)
		}
		//nextDocIDBytes = []byte(strconv.Itoa(0))
	} else if errNext != nil {
		panic(errNext)
	} else {
		nextDocID = nextDocID_.(uint32)
	}
	//nextDocID, err := strconv.Atoi(string(nextDocIDBytes))
	//if err != nil {
	//	panic(err)
	//}

	// check if current doc has an ID
	var docID uint16
	docID_, err := forward[2].Get(ctx, URL)
	if err == badger.ErrKeyNotFound {
		// set docID
		docID = uint16(nextDocID)
		// add this doc to forw[2]
		forward[2].Set(ctx, URL, docID)
		forward[4].Set(ctx, "nextDocID", nextDocID+1)
		nextDocID += 1
	} else { docID = docID_.(uint16) }
	//docID, err := strconv.Atoi(string(docIDBytes))
	//if err != nil {
	//	panic(err)
	//}
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

	var nWID uint32
	nWID_, errNext_ := forward[4].Get(ctx, "nextWordID")
	if errNext_ == badger.ErrKeyNotFound {
		// masukkin 0 as nextWordID
		nWID = 0
		forward[4].Set(ctx, "nextWordID", nWID)
	} else if errNext_ != nil {
		panic(errNext_)
	} else { nWID = nWID_.(uint32) }
	//nWID, err := strconv.Atoi(string(nWIDBytes))
	//if err != nil {
	//	panic(err)
	//}

	for word, _ := range posTitle {
		// save from title wordID -> [{DocID, Pos}]
		setInverted(ctx, word, posTitle, docID, forward, inverted[0], batchDB_frw, batchDB_inv[0], &nWID)
	}
	for word, _ := range posBody {
		// save from body wordID-> [{DocID, Pos}]
		setInverted(ctx, word, posBody, docID, forward, inverted[1], batchDB_frw, batchDB_inv[1], &nWID)
	}

	for _, f := range batchDB_frw {
		f.Flush()
	}
	for _, i := range batchDB_inv {
		i.Flush()
	}

	var kids []uint16
	// get the URL mapping of each child
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

	nextDocID_, err = forward[4].Get(ctx, "nextDocID")
	if err != nil {
		panic(err)
	}
	nextDocID = nextDocID_.(uint32)
	//nextDocID, err = strconv.Atoi(string(nextDocIDBytes))
	//if err != nil {
	//	panic(err)
	//}

	for _, child := range children {
		// fmt.Println(child)
		childURL, err := url.Parse(child)
		if err != nil {
			panic(err)
		}
		mChildURL, errMarshal := childURL.MarshalBinary()
		if errMarshal != nil {
			panic(errMarshal)
		}
		var childID uint16
		childID_, err := forward[2].Get(ctx, childURL)
		if err == badger.ErrKeyNotFound {
			// get the next doc ID
			// nextDocIDBytes, errNext := forward[4].Get(ctx, []byte("nextDocID"))
			// if errNext != nil {
			// 	panic(errNext)
			// }
			// nextDocID, err := strconv.Atoi(string(nDIDBytes))
			// if err != nil {
			// 	panic(err)
			// }
			docInfoC := database.DocInfo{
				*childURL,
				nil,
				time.Now(),
				0,
				nil,
				[]uint16{uint16(docID)},
				nil,
			}
			docInfoBytes, err := json.Marshal(docInfoC)
			if err != nil {
				panic(err)
			}
			// child is not inserted into URL->DocID
			// use writebatch instead
			nDID := strconv.Itoa(int(nextDocID))
			childID = uint16(nextDocID)
			abatchDB_frw[2].Set(mChildURL, []byte(nDID), 0)
			abatchDB_frw[3].Set([]byte(nDID), docInfoBytes, 0)
			// set childID
			// childIDBytes = []byte(nDID)
			// update nextDocID
			abatchDB_frw[4].Set([]byte("nextDocID"), []byte(strconv.Itoa(int(nextDocID)+1)), 0)
			nextDocID += 1
		} else if err != nil {
			panic(err)
		} else { childID = childID_.(uint16) }
		//childID, err := strconv.Atoi(string(childIDBytes))
		//if err != nil {
		//	panic(err)
		//}
		// fmt.Println(childID)
		kids = append(kids, childID)
	}
	err = abatchDB_frw[2].Flush()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	err = abatchDB_frw[3].Flush()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	err = abatchDB_frw[4].Flush()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	// forw[2] save URL -> DocInfo
	// URL to the marshalling stuff
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

	wordMapping := make(map[uint32]uint32)
	for word, val := range freqBody {
		wordID, err := forward[0].Get(ctx, word)
		if err != nil {
			panic(err)
		}
		//wordID, err := strconv.Atoi(string(wordIDBytes))
		//if err != nil {
		//	panic(err)
		//}
		wordMapping[wordID.(uint32)] = val
	}
	var pageInfo database.DocInfo
	if parentURL == "" {
		pageInfo = database.DocInfo{*URL, pageTitle, lastModified, uint32(pageSize), kids, nil, wordMapping}
	} else {
		parentID, err := forward[2].Get(ctx, parentURL)
		if err != nil {
			panic(err)
		}
		//parentID, err := strconv.Atoi(string(parentIDBytes))
		//if err != nil {
		//	panic(err)
		//}
		pageInfo = database.DocInfo{*URL, pageTitle, lastModified, uint32(pageSize), kids, []uint16{parentID.(uint16)}, wordMapping}
	}
	// marshal pageInfo
	//mPageInfo, err := pageInfo.MarshalJSON()
	//if err != nil {
	//	panic(err)
	//}
	// insert into forward 3
	forward[3].Set(ctx, docID, pageInfo)
	// update forward table for DocID and its corresponding URL
	// forward[3].Set(ctx, []byte(strconv.Itoa(nextDocID)), URLBytes)
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
	err = ioutil.WriteFile(docsDir+strconv.Itoa(int(docID)), doc, 0644)
	if err != nil {
		panic(err)
	}
}
