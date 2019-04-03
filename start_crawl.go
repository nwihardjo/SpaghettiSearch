package main

import (
	"the-SearchEngine/crawler"
	"the-SearchEngine/database"
  "the-SearchEngine/indexer"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/apsdehal/go-logger"
	"github.com/eapache/channels"
	"net/http"
	"os"
	"sync"
	"time"
	"strings"
	"encoding/json"
  	"strconv"
)

func main() {
	fmt.Println("Crawler started...")

	start := time.Now()
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	startURL := "https://www.cse.ust.hk/"
	numOfPages := 30
	maxThreadNum := 50
	visited := channels.NewInfiniteChannel()
	queue := channels.NewInfiniteChannel()
	var wg sync.WaitGroup
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

	queue.In() <- []string{"", startURL}

	parentsToBeAdded := make(map[string][]string)

	depth := 0
	nextDepthSize := 1
	fmt.Println("Depth:", depth, "- Queued:", nextDepthSize)

	for visited.Len() < numOfPages {
		for idx := 0; queue.Len() > 0 && idx < maxThreadNum && visited.Len() < numOfPages && nextDepthSize > 0; idx++ {
			if edge, ok := (<-queue.Out()).([]string); ok {

				nextDepthSize -= 1

				parentURL := edge[0]
				currentURL := edge[1]

				/* Check if currentURL is already visited */
				flag := false
				var temp []string // Temporary variable for storing the visited buffer
				for i := 0; i < visited.Len(); i++ {
					v, ok2 := (<-visited.Out()).(string)
					if !ok2 {
						break
					}

					temp = append(temp, v)

					if v == currentURL { // If currentURL in visited, flag = true
						flag = true
					}
				}

				/* Store back the visited URLs from temp to visited buffer */
				for _, t := range temp {
					visited.In() <- t
				}

				/*
					If currentURL is already visited (handle cycle),
					do not visit this URL and do not increase the idx
				*/
				if flag {
					idx--
					if parentsToBeAdded[currentURL] == nil {
						parentsToBeAdded[currentURL] = []string{parentURL}
					} else {
						parentsToBeAdded[currentURL] = append(parentsToBeAdded[currentURL], parentURL)
					}
					continue
				}

				/* Put currentURL to visited buffer */
				visited.In() <- currentURL

				/* Add below goroutine (child) to the list of children to be waited */
				wg.Add(1)

				/* Crawl the URL using goroutine */
				go crawler.Crawl(idx, &wg, parentURL, currentURL,
					client, queue, &mutex, inv, forw)

			} else {
				os.Exit(1)
			}
		}

		/* Wait for all children to finish */
		wg.Wait()

		/*
			Run function AddParent using goroutine
			By running this function after each Wait(),
			it is guaranteed that the original URL with
			the corresponding doc info must have been
			stored in the database and that the parents
			URL are already mapped to some doc id
		*/
		for cURL, parents := range parentsToBeAdded {
			wgIndexer.Add(1)
			go indexer.AddParent(cURL, parents, forw, &wgIndexer)
		}

		/* If finished with current depth level, proceed to the next level */
		if nextDepthSize == 0 {

			depth += 1
			nextDepthSize += queue.Len()
			fmt.Println("Depth:", depth, "- Queued:", nextDepthSize)
		}

		if queue.Len() <= 0 {
			break
		}
	}

	/* Close the visited and queue channels */
	visited.Close()
	queue.Close()

	/* Wait for all indexers to finish */
	wgIndexer.Wait()
	fmt.Println("\nTotal elapsed time: " + time.Now().Sub(start).String())
	//Output into a file
	f, err := os.Create("./spider_result.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Load all data containing DocInfo --> URL
	fin_dat, err := forw[3].Iterate(ctx)
	var final_data []database.DocInfo
	if err != nil {
		panic(err)
	}
	for _, kv := range fin_dat.KV {
		var tempDocInfo database.DocInfo
		err = json.Unmarshal(kv.Value, &tempDocInfo)
		if err != nil {
			panic(err)
		}
		// Remove unscraped data, present for the sake of printing out child url
		if tempDocInfo.Page_size == 0 {
			continue
		}
		final_data = append(final_data, tempDocInfo)
	}

	// Writing result into the file one data at each time
	outputSeparator := "------------------------------------------------------------ \n"
	for _, v := range final_data {
		lineThree := []string{v.Mod_date.Format(time.RFC1123), strconv.Itoa(int(v.Page_size))}
		// Iterate through the keywords and frequency
		wordFreq := []string{}
		for wordId, freq := range v.Words_mapping {
			word, err := forw[1].Get(ctx, []byte(strconv.Itoa(int(wordId))))
			if err != nil {
				panic(err)
			}
			wordFreq = append(wordFreq, string(word)+" "+strconv.Itoa(int(freq)))
		}

		// Iterate through the children of the URL
		childUrl := []string{}
		for _, child := range v.Children{
			var tempData database.DocInfo
			byteDocInfo, err := forw[3].Get(ctx, []byte(strconv.Itoa(int(child))))
			if err != nil {
				panic(err)
			}
			err = json.Unmarshal(byteDocInfo, &tempData)
			if err != nil {
				panic(err)
			}
			childUrl = append(childUrl, "Child "+tempData.Url.String())
		}

		// Append all info for a document into a formatted string to be written
		output := []string{strings.Join(v.Page_title, " "), v.Url.String(), strings.Join(lineThree, ", "), strings.Join(wordFreq, "; "), strings.Join(childUrl, " \n"), outputSeparator}
		_, err := f.WriteString(strings.Join(output, " \n"))
		if err != nil {
			panic(err)
		}
		f.Sync()
	}
	fmt.Println("Finished writing spider_result.txt")

	// word, err:=forw[1].Get(ctx, []byte(strconv.Itoa(9)))
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println(string(word), word)
}
