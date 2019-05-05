package main

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/apsdehal/go-logger"
	"github.com/eapache/channels"
	"github.com/nwihardjo/SpaghettiSearch/crawler"
	"github.com/nwihardjo/SpaghettiSearch/database"
	"github.com/nwihardjo/SpaghettiSearch/ranking"
	"golang.org/x/sync/semaphore"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type URLHash [16]byte

func main() {
	numOfPages := flag.Int("numPages", 300, "-numPages=<number_of_pages_crawled>")
	startURL := flag.String("startURL", "https://www.cse.ust.hk", "-startURL=<crawler_entry_point>")
	domainOnly := flag.Bool("domainOnly", true, "-domainOnly=<crawl_only_domain_given_domain_or_not>")
	flag.Parse()

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
		Timeout:   td,
	}

	var domain string
	if temp, err := url.Parse(*startURL); err != nil {
		panic(err)
	} else {
		domain = temp.Hostname()
	}

	maxThreadNum := 300
	sem := semaphore.NewWeighted(int64(maxThreadNum))
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

	queue.In() <- []string{"", *startURL}

	depth := 0
	nextDepthSize := 1
	fmt.Println("Depth:", depth, "- Queued:", nextDepthSize)

	for len(visited) < *numOfPages {
		for queue.Len() > 0 && len(visited) < *numOfPages && nextDepthSize > 0 {
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
				if !strings.HasSuffix(u.Hostname(), domain) && *domainOnly {
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
				*numOfPages += 1
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
	fmt.Println("\nTotal elapsed time: ", time.Now().Sub(start).String())
}
