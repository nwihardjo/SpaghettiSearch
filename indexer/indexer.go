package indexer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/dgraph-io/badger"
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"the-SearchEngine/database"
	"the-SearchEngine/parser"
	"time"
)

var DocsDir = "docs/"

func Index(doc []byte, urlString string, lock2 *sync.RWMutex,
	lastModified time.Time, ps string, mutex *sync.Mutex,
	inverted []database.DB, forward []database.DB,
	parentURL string, children []string) {

	ctx, _ := context.WithCancel(context.TODO())

	// Get the URL type of current URL string
	URL, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	fmt.Println("Indexing", URL.String())

	// Get the hash of current URL
	docHash := md5.Sum([]byte(urlString))
	docHashString := hex.EncodeToString(docHash[:])

	// Get Last Modified from DB
	var dI database.DocInfo
	dI_, err := forward[1].Get(ctx, docHashString)
	checkIndex := false
	updateTitle := false
	updateBody := false
	updateKids := false
	if err == nil {
		dI, ok := dI_.(database.DocInfo)
		if !ok {
			panic("Type assertion failed")
		}
		lm := dI.Mod_date
		if lastModified.After(lm) {
			// check dI different or not
			// if same, no need to update
			// else, delet first then set
			// if last modified is zero -> only a dummy DocInfo
			if lm.IsZero() {
				checkIndex = false
			} else {
				checkIndex = true
			}
		} else {
			// no need to update
			fmt.Println("\n\n[DEBUG] NO UPDATE NEEDED\n\n")
			return
		}
	} else if err == badger.ErrKeyNotFound {
		// do indexing as usual
		checkIndex = false
	} else {
		panic(err)
	}

	// title and body are structs
	titleInfo, bodyInfo := parser.Parse(doc)

	// Parse title & page size
	pageTitle := strings.Fields(titleInfo.Content)
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
	for word, val := range bodyInfo.Freq {
		h := md5.Sum([]byte(word))
		wordMapping[hex.EncodeToString(h[:])] = val
	}

	// Init batch writer for modified handler
	var bwFrw []database.BatchWriter
	var bwFrw []database.BatchWriter
	var bwInv []database.BatchWriter

	for _, i := range forward {
		temp := i.BatchWrite_init(ctx)
		defer temp.Cancel(ctx)
		bwFrw = append(bwFrw, temp)
	}
	for _, i := range inverted {
		temp := i.BatchWrite_init(ctx)
		defer temp.Cancel(ctx)
		bwInv = append(bwInv, temp)
	}

	// Initialize container for docHashes of children
	var kids []string
	var kidUrls []*url.URL

	for _, child := range children {
		// Get URL object of current child url
		childURL, err := url.Parse(child)
		if err != nil {
			panic(err)
		}

		// Get docHash of each child
		childHash := md5.Sum([]byte(child))
		childHashString := hex.EncodeToString(childHash[:])

		kids = append(kids, childHashString)
		kidUrls = append(kidUrls, childURL)
	}


	// If the doc exists, check its title, body, children, and page size
	// If any of them modified, update / delete accordingly
	if checkIndex {
		checkAndUpdate(&dI, pageTitle, kids, lock2, docHashString,
			bwInv, bwFrw, wordMapping, pageSize, inverted, forward,
			ctx, &updateTitle, &updateBody, &updateKids)
	}

	// If the doc exists and there is no changes, return
	if checkIndex && !updateTitle && !updateBody && !updateKids {
		fmt.Println("\n\n[DEBUG] Checked, no update\n\n")
		return
	}

	// initiate batch object
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

	lock2.RLock()
	// if current doc is not found or if the new title is different from the old one,
	// process and load data to batch writer for inverted tables
	// map word to wordHash as well if not exist
	if !checkIndex || updateTitle {
		maxFreq := getMaxFreq(titleInfo.Freq)
		for word, _ := range titleInfo.Pos {
			// save from title wordHash -> [{DocHash, Positions}]
			setInverted(ctx, word, titleInfo.Pos, maxFreq, docHashString, forward, inverted[0], batchWriter_forward, batchWriter_inverted[0], mutex)
		}
	}

	if !checkIndex || updateBody {
		maxFreq := getMaxFreq(bodyInfo.Freq)
		for word, _ := range bodyInfo.Pos {
			// save from body wordHash-> [{DocHash, Positions}]
			setInverted(ctx, word, bodyInfo.Pos, maxFreq, docHashString, forward, inverted[1], batchWriter_forward, batchWriter_inverted[1], mutex)
		}
	}
	lock2.RUnlock()

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

	if !checkIndex || updateKids {
		for idx, kid := range kids {
			// Get DocInfo corresponding to the child,
			// make one if not present (for the sake of getting the url of not-yet-visited child)
			docInfoC, err := forward[1].Get(ctx, kid)
			if err == badger.ErrKeyNotFound {
				docInfoC = database.DocInfo{*kidUrls[idx], nil, time.Time{}, 0, nil, []string{kid}, nil}

				// Set docHash of child -> docInfo of child using batch writer
				if err = bw_child.BatchSet(ctx, kid, docInfoC); err != nil {
					panic(err)
				}
			} else if err != nil {
				panic(err)
			}
		}

		// Store the children of current doc to db for faster pagerank process
		if err = forward[2].Set(ctx, docHashString, kids); err != nil {
			panic(err)
		}
	}

	// Save children data into the db
	if err = bw_child.Flush(ctx); err != nil {
		panic(err)
	}

	// PageInfo
	// Initialize document object
	}

	// If the doc exists and there is no changes, return
	if checkIndex && !updateTitle && !updateBody && !updateKids {
		fmt.Println("\n\n[DEBUG] Checked, no update\n\n")
		return
	}

	// initiate batch object
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

	lock2.RLock()
	// if current doc is not found or if the new title is different from the old one,
	// process and load data to batch writer for inverted tables
	// map word to wordHash as well if not exist
	if !checkIndex || updateTitle {
		maxFreq := getMaxFreq(titleInfo.Freq)
		for word, _ := range titleInfo.Pos {
			// save from title wordHash -> [{DocHash, Positions}]
			setInverted(ctx, word, titleInfo.Pos, maxFreq, docHashString, forward, inverted[0], batchWriter_forward, batchWriter_inverted[0], mutex)
		}
	}

	if !checkIndex || updateBody {
		maxFreq := getMaxFreq(bodyInfo.Freq)
		for word, _ := range bodyInfo.Pos {
			// save from body wordHash-> [{DocHash, Positions}]
			setInverted(ctx, word, bodyInfo.Pos, maxFreq, docHashString, forward, inverted[1], batchWriter_forward, batchWriter_inverted[1], mutex)
		}
	}
	lock2.RUnlock()

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

	if !checkIndex || updateKids {
		for idx, kid := range kids {
			// Get DocInfo corresponding to the child,
			// make one if not present (for the sake of getting the url of not-yet-visited child)
			docInfoC, err := forward[1].Get(ctx, kid)
			if err == badger.ErrKeyNotFound {
				docInfoC = database.DocInfo{*kidUrls[idx], nil, time.Time{}, 0, nil, []string{kid}, nil}

				// Set docHash of child -> docInfo of child using batch writer
				if err = bw_child.BatchSet(ctx, kid, docInfoC); err != nil {
					panic(err)
				}
			} else if err != nil {
				panic(err)
			}
		}

		// Store the children of current doc to db for faster pagerank process
		if err = forward[2].Set(ctx, docHashString, kids); err != nil {
			panic(err)
		}
	}

	// Save children data into the db
	if err = bw_child.Flush(ctx); err != nil {
		panic(err)
	}

	// PageInfo
	// Initialize document object
	var pageInfo database.DocInfo
	if checkIndex {
		pageInfo = dI
		if updateTitle {
			pageInfo.Page_title = pageTitle
		}
		if updateBody {
			pageInfo.Words_mapping = wordMapping
		}
		if updateKids {
			pageInfo.Children = kids
		}
		pageInfo.Mod_date = lastModified
		pageInfo.Page_size = uint32(pageSize)
	} else {
		if parentURL == "" {
			pageInfo = database.DocInfo{*URL, pageTitle, lastModified, uint32(pageSize), kids, nil, wordMapping}
		} else {
			pHash := md5.Sum([]byte(parentURL))
			pHashString := hex.EncodeToString(pHash[:])
			pageInfo = database.DocInfo{*URL, pageTitle, lastModified, uint32(pageSize), kids, []string{pHashString}, wordMapping}
		}
	}

	// Save docHash -> docInfo of current doc
	if err = forward[1].Set(ctx, docHashString, pageInfo); err != nil {
		panic(err)
	}

	// Cache
	if _, err := os.Stat(DocsDir); os.IsNotExist(err) {
		os.Mkdir(DocsDir, 0755)
	}
	if err = ioutil.WriteFile(DocsDir+docHashString, doc, 0644); err != nil {
		panic(err)
	}
}

func checkAndUpdate(dI *database.DocInfo, pageTitle, kids []string, lock2 *sync.RWMutex, docHashString string,
	bwInv []database.BatchWriter, bwFrw []database.BatchWriter, wordMapping map[string]uint32, pageSize int,
	inverted, forward []database.DB, ctx context.Context, updateTitle, updateBody, updateKids *bool) {

	// Check the doc title and remove anything related to this docHash
	// from the titla inverted table if changed
	if !reflect.DeepEqual(dI.Page_title, pageTitle) {
		lock2.Lock()
		for _, word := range dI.Page_title {
			h := md5.Sum([]byte(word))
			hStr := hex.EncodeToString(h[:])
			docP_, e := inverted[0].Get(ctx, hStr)
			if e != nil {
				panic(e)
			}
			docP, ok := docP_.(map[string][]float32)
			if !ok {
				panic("Type assertion failed")
			}
			if len(docP) > 1 {
				// remove this doc from this row
				delete(docP, docHashString)
				if e = bwInv[0].BatchSet(ctx, hStr, docP); e != nil {
					panic(e)
				}
			} else {
				// delete this row
				if e = inverted[0].Delete(ctx, hStr); e != nil {
					panic(e)
				}
			}
		}
		lock2.Unlock()
		*updateTitle = true
	}

	// Check the doc body and remove anything related to this docHash
	// from the body inverted table if changed
	if !reflect.DeepEqual(dI.Words_mapping, wordMapping) {
		lock2.Lock()
		for word, _ := range dI.Words_mapping {
			h := md5.Sum([]byte(word))
			hStr := hex.EncodeToString(h[:])
			docP_, e := inverted[1].Get(ctx, hStr)
			if e != nil {
				panic(e)
			}
			docP, ok := docP_.(map[string][]float32)
			if !ok {
				panic("Type assertion failed")
			}
			if len(docP) > 1 {
				// remove this doc from this row
				delete(docP, docHashString)
				if e = bwInv[1].BatchSet(ctx, hStr, docP); e != nil {
					panic(e)
				}
			} else {
				// delete this row
				if e = inverted[1].Delete(ctx, hStr); e != nil {
					panic(e)
				}
			}
		}
		lock2.Unlock()
		*updateBody = true
	}

	// Check the doc children and delete the docHash of this page
	// from all the children's Parent if the children is changed
	if !reflect.DeepEqual(dI.Children, kids) {
		for _, c := range dI.Children {
			dIc_, e := forward[1].Get(ctx, c)
			if e != nil {
				panic(e)
			}
			dIc, ok := dIc_.(database.DocInfo)
			if !ok {
				panic("Type assertion failed")
			}
			tempParents := dIc.Parents[:]
			dIc.Parents = make([]string, len(tempParents) - 1)
			for _, t := range tempParents {
				if t != docHashString {
					dIc.Parents = append(dIc.Parents, t)
				}
			}
			if e = bwFrw[1].BatchSet(ctx, c, dIc); e != nil {
				panic(e)
			}
		}
		*updateKids = true
	}

	// Check the doc size and update if changed
	if dI.Page_size != uint32(pageSize) {
		dI.Page_size = uint32(pageSize)
		if !*updateTitle && !*updateBody && !*updateKids {
			if e := forward[1].Set(ctx, docHashString, dI); e != nil {
				panic(e)
			}
		}
	}

	// Flush the writes
	for _, f := range bwFrw {
		if err := f.Flush(ctx); err != nil {
			panic(err)
		}
	}
	for _, i := range bwInv {
		if err := i.Flush(ctx); err != nil {
			panic(err)
		}
	}
}

func setInverted(ctx context.Context, word string, pos map[string][]float32, maxFreq uint32, docHash string, forward []database.DB, inverted database.DB, bw_forward []database.BatchWriter, bw_inverted database.BatchWriter, mutex *sync.Mutex) {

	// initialise inverted keywords values
	invKeyVals := make(map[string][]float32)
	normTF := float32(len(pos[word])) / float32(maxFreq)
	invKeyVals[docHash] = append([]float32{normTF}, pos[word]...)

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
		value.(map[string][]float32)[docHash] = invKeyVals[docHash]

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
		fmt.Println(docHashString, "=", currentURL_)
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

func getMaxFreq(in map[string]uint32) (ret uint32) {
	ret = 0
	for _, v := range in {
		if v > ret {
			ret = v
		}
	}
	return
}
