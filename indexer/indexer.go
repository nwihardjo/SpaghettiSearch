package indexer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/nwihardjo/SpaghettiSearch/database"
	"github.com/nwihardjo/SpaghettiSearch/parser"
	"golang.org/x/net/html"
	"io/ioutil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var DocsDir = "docs/"

func Index(doc []byte, rootNode *html.Node, urlString string,
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
	mutex.Lock()
	dI_, err := forward[1].Get(ctx, docHashString)
	checkIndex := false
	if err == nil {
		dI = dI_.(database.DocInfo)
		lm := dI.Mod_date
		if lastModified.After(lm) {
			// check dI different or not
			// if same, no need to update
			// else, delete first then set
			// if last modified is zero -> only a dummy DocInfo
			if lm.IsZero() {
				checkIndex = false
			} else {
				checkIndex = true
			}
		} else {
			// no need to update
			mutex.Unlock()
			return
		}
	} else if err == badger.ErrKeyNotFound {
		// do indexing as usual
		checkIndex = false
	} else {
		panic(err)
	}
	mutex.Unlock()

	// If the doc exists, check its title, body, children, and page size
	// If any of them modified, update / delete accordingly
	if checkIndex {
		checkAndUpdate(mutex, docHashString, dI, &checkIndex, doc, inverted, forward)
	}

	// title and body are structs
	titleInfo, bodyInfo, fancyInfo, cleanFancy := parser.Parse(rootNode, urlString)

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

	// process and load data to batch writer for inverted tables
	// map word to wordHash as well if not exist
	maxFreq := getMaxFreq(titleInfo.Freq)
	// save from title wordHash -> [{DocHash, Positions}]
	mutex.Lock()
	setInverted(ctx, titleInfo.Pos, maxFreq, docHashString, forward, inverted[0], batchWriter_forward, batchWriter_inverted[0])

	maxFreq = getMaxFreq(bodyInfo.Freq)
	// save from body wordHash-> [{DocHash, Positions}]
	setInverted(ctx, bodyInfo.Pos, maxFreq, docHashString, forward, inverted[1], batchWriter_forward, batchWriter_inverted[1])

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
	mutex.Unlock()

	// initialise batch writer for child append
	bw_child := forward[1].BatchWrite_init(ctx)
	defer bw_child.Cancel(ctx)

	mutex.Lock()
	for idx, kid := range kids {
		bw_anchor := inverted[0].BatchWrite_init(ctx)
		defer bw_anchor.Cancel(ctx)
		bw_anchor_frw := forward[0].BatchWrite_init(ctx)
		defer bw_anchor_frw.Cancel(ctx)

		// Get DocInfo corresponding to the child,
		// make one if not present (for the sake of getting the url of not-yet-visited child)
		docInfoC, err := forward[1].Get(ctx, kid)
		if err == badger.ErrKeyNotFound {
			tempP := make(map[string][]string)
			if cleanFancy[kid] == nil {
				tempP[docHashString] = []string{}
			} else {
				tempP[docHashString] = cleanFancy[kid]
			}
			docInfoC_ := database.DocInfo{*kidUrls[idx], nil, time.Time{}, 0, nil, tempP, nil}

			// Set docHash of child -> docInfo of child using batch writer
			if err = bw_child.BatchSet(ctx, kid, docInfoC_); err != nil {
				panic(err)
			}

			tttt := make(map[string]uint32)
			babi := make(map[string][]float32)
			for _, w := range cleanFancy[kid] {
				tttt[w] += 1
				babi[w] = append(babi[w], -100)
			}
			maxFreq := getMaxFreq(fancyInfo[kid].Freq)
			var wg1 sync.WaitGroup
			for wrd, _ := range tttt {
				wg1.Add(1)
				go func(w string) {
					defer wg1.Done()
					wHash := md5.Sum([]byte(w))
					wHashString := hex.EncodeToString(wHash[:])
					invKeyVals := make(map[string][]float32)
					normTF := float32(float32(tttt[w]) / float32(maxFreq))
					invKeyVals[kid] = append([]float32{normTF}, babi[w]...)
					// append the added entry (docHash and pos) to inverted file
					// value has type of map[DocHash][]uint32 (docHash -> list of position)
					value, err := inverted[0].Get(ctx, wHashString)
					if err == badger.ErrKeyNotFound {
						// there's no entry on the inverted table for the corresponding wordHash
						if err = bw_anchor.BatchSet(ctx, wHashString, invKeyVals); err != nil {
							panic(err)
						}
						if err = bw_anchor_frw.BatchSet(ctx, wHashString, w); err != nil {
							panic(err)
						}
					} else if err != nil {
						panic(err)
					} else {
						// append new docHash entry to the existing one
						value.(map[string][]float32)[kid] = invKeyVals[kid]

						// load new appended value of inverted table according to the wordHash
						if err = bw_anchor.BatchSet(ctx, wHashString, value); err != nil {
							panic(err)
						}
					}
				}(wrd)
			}
			wg1.Wait()
		} else if err != nil {
			panic(err)
		} else {
			docInfoC_ := docInfoC.(database.DocInfo)
			if docInfoC_.Parents == nil {
				docInfoC_.Parents = make(map[string][]string)
			}
			docInfoC_.Parents[docHashString] = cleanFancy[kid]
			// Set docHash of child -> docInfo of child using batch writer
			if err = bw_child.BatchSet(ctx, kid, docInfoC_); err != nil {
				panic(err)
			}
			tttt := make(map[string]uint32)
			babi := make(map[string][]float32)
			for _, w := range cleanFancy[kid] {
				tttt[w] += 1
				babi[w] = append(babi[w], -100)
			}
			tempCleanFancyUnique := tttt
			for i, w := range docInfoC_.Page_title {
				tttt[w] += 1
				babi[w] = append(babi[w], float32(i))
			}
			maxFreq := uint32(0)
			for _, v := range tttt {
				if v > maxFreq {
					maxFreq = v
				}
			}
			var wg1 sync.WaitGroup
			for wrd, _ := range tempCleanFancyUnique {
				wg1.Add(1)
				go func(w string) {
					defer wg1.Done()
					wHash := md5.Sum([]byte(w))
					wHashString := hex.EncodeToString(wHash[:])
					invKeyVals := make(map[string][]float32)
					normTF := float32(float32(tttt[w]) / float32(maxFreq))
					invKeyVals[kid] = append([]float32{normTF}, babi[w]...)
					// append the added entry (docHash and pos) to inverted file
					// value has type of map[DocHash][]uint32 (docHash -> list of position)
					value, err := inverted[0].Get(ctx, wHashString)
					if err == badger.ErrKeyNotFound {
						// there's no entry on the inverted table for the corresponding wordHash
						if err = bw_anchor.BatchSet(ctx, wHashString, invKeyVals); err != nil {
							panic(err)
						}
						if err = bw_anchor_frw.BatchSet(ctx, wHashString, w); err != nil {
							panic(err)
						}
					} else if err != nil {
						panic(err)
					} else {
						// append new docHash entry to the existing one
						value.(map[string][]float32)[kid] = invKeyVals[kid]

						// load new appended value of inverted table according to the wordHash
						if err = bw_anchor.BatchSet(ctx, wHashString, value); err != nil {
							panic(err)
						}
					}
				}(wrd)
			}
			wg1.Wait()
		}
		if err := bw_anchor.Flush(ctx); err != nil {
			panic(err)
		}
		if err := bw_anchor_frw.Flush(ctx); err != nil {
			panic(err)
		}
	}

	// Store the children of current doc to db for faster pagerank process
	if err = forward[2].Set(ctx, docHashString, kids); err != nil {
		panic(err)
	}

	// Save children data into the db
	if err = bw_child.Flush(ctx); err != nil {
		panic(err)
	}
	mutex.Unlock()

	// PageInfo
	// Initialize document object
	var pageInfo database.DocInfo
	if checkIndex {
		pageInfo = dI
		pageInfo.Page_title = pageTitle
		pageInfo.Words_mapping = wordMapping
		pageInfo.Children = kids
		pageInfo.Mod_date = lastModified
		pageInfo.Page_size = uint32(pageSize)
	} else {
		if parentURL == "" {
			pageInfo = database.DocInfo{*URL, pageTitle, lastModified, uint32(pageSize), kids, nil, wordMapping}
		} else {
			pHash := md5.Sum([]byte(parentURL))
			pHashString := hex.EncodeToString(pHash[:])
			tempP := make(map[string][]string)
			tempP[pHashString] = []string{}
			pageInfo = database.DocInfo{*URL, pageTitle, lastModified, uint32(pageSize), kids, tempP, wordMapping}
		}
	}

	mutex.Lock()
	// Save docHash -> docInfo of current doc
	if err = forward[1].Set(ctx, docHashString, pageInfo); err != nil {
		panic(err)
	}
	mutex.Unlock()

	// Cache
	if _, err := os.Stat(DocsDir); os.IsNotExist(err) {
		os.Mkdir(DocsDir, 0755)
	}
	if err = ioutil.WriteFile(DocsDir+docHashString, doc, 0644); err != nil {
		panic(err)
	}
}

func setInverted(ctx context.Context, pos map[string][]float32, maxFreq uint32, docHash string,
	forward []database.DB, inverted database.DB, bw_forward []database.BatchWriter, bw_inverted database.BatchWriter) {

	var wg1 sync.WaitGroup
	for w, _ := range pos {

		wg1.Add(1)
		go func(word string) {
			defer wg1.Done()

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
		}(w)

	}
	wg1.Wait()

	return
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

func checkAndUpdate(mutex *sync.Mutex, docHashString string, dI database.DocInfo, checkIndex *bool,
	doc []byte, inverted []database.DB, forward []database.DB) {

	cacheFileD, e := ioutil.ReadFile(DocsDir + docHashString)
	if e != nil {
		fmt.Println(e)
		*checkIndex = false
	} else {
		cacheFileDHash := md5.Sum(cacheFileD)
		currentDocHash := md5.Sum(doc)
		if currentDocHash != cacheFileDHash {
			ctx, _ := context.WithCancel(context.Background())
			// Init batch writer for modified handler
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

			type DocPosHashStruct struct {
				DocPos   map[string][]float32
				WordHash string
			}
			tempPageTitle := parser.Laundry(strings.Join(dI.Page_title, " "))
			wordChann := make(chan DocPosHashStruct, len(tempPageTitle))
			var wgGet sync.WaitGroup
			mutex.Lock()
			for _, word := range tempPageTitle {
				h := md5.Sum([]byte(word))
				hStr := hex.EncodeToString(h[:])
				wgGet.Add(1)
				go func(hS string) {
					defer wgGet.Done()
					docP_, e := inverted[0].Get(ctx, hS)
					if e != nil {
						fmt.Println(e)
						wordChann <- DocPosHashStruct{nil, ""}
					} else {
						docP, _ := docP_.(map[string][]float32)
						wordChann <- DocPosHashStruct{docP, hS}
					}
				}(hStr)
			}

			wgGet.Wait()
			close(wordChann)
			for dphs := range wordChann {
				docP := dphs.DocPos
				hStr := dphs.WordHash
				if hStr == "" {
					continue
				}
				if len(docP) > 1 {
					// remove this doc from this row
					delete(docP, docHashString)
					if e = bwInv[0].BatchSet(ctx, hStr, docP); e != nil {
						panic(e)
					}
				} else if docP[docHashString] != nil {
					// delete this row
					if e = inverted[0].Delete(ctx, hStr); e != nil {
						panic(e)
					}
				}
			}

			wordChann = make(chan DocPosHashStruct, len(dI.Words_mapping))
			for wordHash, _ := range dI.Words_mapping {
				wgGet.Add(1)
				go func(whS string) {
					defer wgGet.Done()
					docP_, e := inverted[1].Get(ctx, whS)
					if e != nil {
						fmt.Println(e)
						wordChann <- DocPosHashStruct{nil, ""}
					} else {
						docP, _ := docP_.(map[string][]float32)
						wordChann <- DocPosHashStruct{docP, whS}
					}
				}(wordHash)
			}

			wgGet.Wait()
			close(wordChann)
			for dphs := range wordChann {
				docP := dphs.DocPos
				wordHash := dphs.WordHash
				if wordHash == "" {
					continue
				}
				if len(docP) > 1 {
					// remove this doc from this row
					delete(docP, docHashString)
					if e = bwInv[1].BatchSet(ctx, wordHash, docP); e != nil {
						panic(e)
					}
				} else if docP[docHashString] != nil {
					// delete this row
					if e = inverted[1].Delete(ctx, wordHash); e != nil {
						panic(e)
					}
				}
			}

			type DocInfoChildStruct struct {
				DocInfo   database.DocInfo
				ChildHash string
			}
			newChann := make(chan DocInfoChildStruct, len(dI.Children))
			for _, c := range dI.Children {
				wgGet.Add(1)
				go func(cHash string) {
					defer wgGet.Done()
					dIc_, e := forward[1].Get(ctx, cHash)
					if e != nil {
						panic(e)
					}
					dIc, _ := dIc_.(database.DocInfo)
					newChann <- DocInfoChildStruct{dIc, c}
				}(c)
			}

			wgGet.Wait()
			close(newChann)
			type DocPosHashChildStruct struct {
				DocPos    map[string][]float32
				WordHash  string
				ChildHash string
			}
			arrOfChann := make([]chan DocPosHashChildStruct, len(dI.Children))
			arrOfWGs := make([]sync.WaitGroup, len(dI.Children))
			arrIdx := -1
			for dIcs := range newChann {
				dIc := dIcs.DocInfo
				c := dIcs.ChildHash
				arrIdx += 1
				tempParents := dIc.Parents
				dIc.Parents = make(map[string][]string)
				var innerWordHashes []string

				for k, t := range tempParents {
					if k != docHashString {
						dIc.Parents[k] = t
					} else {
						innerWordHashes = t
					}
				}
				if e = bwFrw[1].BatchSet(ctx, c, dIc); e != nil {
					panic(e)
				}

				arrOfChann[arrIdx] = make(chan DocPosHashChildStruct, len(innerWordHashes))
				// arrOfWGs[arrIdx] = sync.WaitGroup

				for _, w := range innerWordHashes {
					arrOfWGs[arrIdx].Add(1)

					wHash := md5.Sum([]byte(w))
					wHashString := hex.EncodeToString(wHash[:])
					go func(wHStr string, childHash string, idx int) {
						defer arrOfWGs[idx].Done()
						dpw_, e := inverted[0].Get(ctx, wHStr)
						if e != nil {
							panic(e)
						}
						dpw, _ := dpw_.(map[string][]float32)
						arrOfChann[idx] <- DocPosHashChildStruct{dpw, wHStr, childHash}
					}(wHashString, c, arrIdx)
				}
			}
			for i, _ := range arrOfWGs {
				arrOfWGs[i].Wait()
			}
			for _, channC := range arrOfChann {
				close(channC)

				for dphs := range channC {
					dpw := dphs.DocPos
					wHashString := dphs.WordHash
					childHash := dphs.ChildHash
					if len(dpw) > 1 {
						// remove this doc from this row
						delete(dpw, childHash)
						if e = bwInv[0].BatchSet(ctx, wHashString, dpw); e != nil {
							panic(e)
						}
					} else if dpw[childHash] != nil {
						// delete this row
						if e = inverted[0].Delete(ctx, wHashString); e != nil {
							panic(e)
						}
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
			mutex.Unlock()
		} else {
			// If the doc exists and there is no changes, return
			// no need to update
			return
		}
	}
}
