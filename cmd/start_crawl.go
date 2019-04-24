package main

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"fmt"
	"github.com/apsdehal/go-logger"
	"github.com/eapache/channels"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"the-SearchEngine/crawler"
	"the-SearchEngine/database"
	"the-SearchEngine/indexer"
	"the-SearchEngine/ranking"
	"time"
)

type URLHash [16]byte

func main() {
	fmt.Println("Crawler started...")

	start := time.Now()
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	startURL := "https://www.cse.ust.hk"
	numOfPages := 300
	maxThreadNum := 100
	domain := "ust.hk"
	unreachableURLs := make(map[string]bool)
	visited := make(map[URLHash]bool)
	queue := channels.NewInfiniteChannel()
	errorsChannel := channels.NewInfiniteChannel()
	var wg sync.WaitGroup
	var wgIndexer sync.WaitGroup
	var mutex sync.Mutex
	var lock2 sync.RWMutex

	ctx, cancel := context.WithCancel(context.TODO())
	log, _ := logger.New("test", 1)
	inv, forw, _ := database.DB_init(ctx, log)
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

	for len(visited) < numOfPages {
		for idx := 0; queue.Len() > 0 && idx < maxThreadNum && len(visited) < numOfPages && nextDepthSize > 0; idx++ {
			if edge, ok := (<-queue.Out()).([]string); ok {

				nextDepthSize -= 1

				parentURL := edge[0]
				currentURL := edge[1]

				/* Check if currentURL is already visited */
				if visited[md5.Sum([]byte(currentURL))] {
					/*
						If currentURL is already visited (handle cycle),
						do not visit this URL and do not increase the idx
					*/
					idx--
					parentsToBeAdded[currentURL] = append(parentsToBeAdded[currentURL], parentURL)
					continue
				}

				/* If currentURL is not in the specified domain, skip it */
				u, e := url.Parse(currentURL)
				if e != nil {
					panic(e)
				}
				if !strings.HasSuffix(u.Hostname(), domain) {
					idx--
					continue
				}

				/* Put currentURL to visited buffer */
				visited[md5.Sum([]byte(currentURL))] = true

				/* Add below goroutine (child) to the list of children to be waited */
				wg.Add(1)

				/* Crawl the URL using goroutine */
				go crawler.Crawl(idx, &wg, parentURL, currentURL, errorsChannel,
					client, &lock2, queue, &mutex, inv, forw)

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
		for errorsChannel.Len() > 0 {
			if x, ok := (<-errorsChannel.Out()).(string); ok {
				unreachableURLs[x] = true
			} else {
				os.Exit(1)
			}
		}
		wgIndexer.Wait()
		for cURL, parents := range parentsToBeAdded {
			if !unreachableURLs[cURL] {
				wgIndexer.Add(1)
				go indexer.AddParent(cURL, parents, forw, &wgIndexer)
			}
		}

		/* If finished with current depth level, proceed to the next level */
		if nextDepthSize == 0 {

			// Maybe also sync DB per level???
			depth += 1
			nextDepthSize += queue.Len()
			fmt.Println("Depth:", depth, "- Queued:", nextDepthSize)
		}

		if queue.Len() <= 0 {
			fmt.Println("\n\n[DEBUG] QUEUE EMPTY\n\n")
			break
		}
	}

	/* Close the queue channel */
	queue.Close()

	/* Wait for all indexers to finish */
	wgIndexer.Wait()
	fmt.Println("\nTotal visited length:", len(visited))
	fmt.Println("\nTotal elapsed time: " + time.Now().Sub(start).String())
	
	timer := time.Now()
	ranking.UpdatePagerank(ctx, 0.85, 0.000001, forw) 
	fmt.Println("Updating pagerank (including read and write to db) takes", time.Since(timer))
}
