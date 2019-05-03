package main

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"fmt"
	"github.com/apsdehal/go-logger"
	"github.com/eapache/channels"
	"golang.org/x/sync/semaphore"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"the-SearchEngine/crawler"
	"the-SearchEngine/database"
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
	td, timeE := time.ParseDuration("15s")
	if timeE != nil {
		panic(timeE)
	}
	client := &http.Client{
		Transport: tr,
		Timeout: td,
	}

	startURL := "https://www.cse.ust.hk"
	numOfPages := 500
	maxThreadNum := 300
	sem := semaphore.NewWeighted(int64(maxThreadNum))
	domain := "cse.ust.hk"
	visited := make(map[URLHash]bool)
	queue := channels.NewInfiniteChannel()
	errorsChannel := channels.NewInfiniteChannel()
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

	depth := 0
	nextDepthSize := 1
	fmt.Println("Depth:", depth, "- Queued:", nextDepthSize)

	for len(visited) < numOfPages {
		for queue.Len() > 0 && len(visited) < numOfPages && nextDepthSize > 0 {
			if edge, ok := (<-queue.Out()).([]string); ok {

				nextDepthSize -= 1

				parentURL := edge[0]
				currentURL := edge[1]

				/* Check if currentURL is already visited */
				if visited[md5.Sum([]byte(currentURL))] {
					/*
						If currentURL is already visited (handle cycle),
						do not visit this URL
					*/
					continue
				}

				/* If currentURL is not in the specified domain, skip it */
				u, e := url.Parse(currentURL)
				if e != nil {
					panic(e)
				}
				if !strings.HasSuffix(u.Hostname(), domain) {
					continue
				}

				/* Put currentURL to visited buffer */
				visited[md5.Sum([]byte(currentURL))] = true

				/* Add below goroutine (child) to the list of children to be waited */
				if e = sem.Acquire(ctx, 1); e != nil {
					panic(e)
				}

				/* Crawl the URL using goroutine */
				go crawler.Crawl(sem, parentURL, currentURL, errorsChannel,
					client, &lock2, queue, &mutex, inv, forw)

			} else {
				os.Exit(1)
			}
		}

		/* Wait for all children to finish */
		if e := sem.Acquire(ctx, int64(maxThreadNum)); e != nil {
			panic(e)
		}

		/*
			Run function AddParent using goroutine
			By running this function after each Wait(),
			it is guaranteed that the original URL with
			the corresponding doc info must have been
			stored in the database and that the parents
			URL are already mapped to some doc id
		*/
		for errorsChannel.Len() > 0 {
			if _, ok := (<-errorsChannel.Out()).(string); ok {
				numOfPages += 1
			} else {
				os.Exit(1)
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

		sem.Release(int64(maxThreadNum))
	}

	/* Close the queue channel */
	queue.Close()

	fmt.Println("\nTotal visited length:", len(visited))
	fmt.Println("\nTotal crawling and indexing time: " + time.Now().Sub(start).String())

	//inv[1].Debug_Print(ctx)
	// perform database update
	timer := time.Now()
	ranking.UpdatePagerank(ctx, 0.85, 1e-20, forw)
	ranking.UpdateTermWeights(ctx, &inv[0], forw, "title")
	ranking.UpdateTermWeights(ctx, &inv[1], forw, "body")

	//inv[1].Debug_Print(ctx)
	fmt.Println("Updating pagerank and idf takes", time.Since(timer))
	fmt.Println("\nTotal elapsed time: " ,time.Now().Sub(start).String())
}
