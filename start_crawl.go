package main

import (
	"./crawler"
	"./database"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/apsdehal/go-logger"
	"github.com/eapache/channels"
	"net/http"
	"os"
	"sync"
	"time"
	// "strconv"

)

func main() {
	fmt.Println("Crawler started...")

	start := time.Now()

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	startURL := "https://www.cse.ust.hk/"
	numOfPages := 3
	maxThreadNum := 10
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

	queue.In() <- startURL

	for visited.Len() < numOfPages {
		for idx := 0; queue.Len() > 0 && idx < maxThreadNum && visited.Len() < numOfPages; idx++ {
			if currentURL, ok := (<-queue.Out()).(string); ok {

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
					continue
				}

				/* Put currentURL to visited buffer */
				visited.In() <- currentURL

				/* Add below goroutine (child) to the list of children to be waited */
				wg.Add(1)

				/* Crawl the URL using goroutine */
				go crawler.Crawl(idx, &wg, &wgIndexer, currentURL, client, queue, &mutex, inv, forw)

			} else {
				os.Exit(1)
			}
		}
		fmt.Println("1life is confusing")

		/* Wait for all children to finish */
		wg.Wait()
		fmt.Println("2life is confusing")

		if queue.Len() <= 0 {
			break
		}
	}

	/* Close the visited and queue channels */
	visited.Close()
	queue.Close()
	fmt.Println("life is confusing")
	wgIndexer.Wait()
	fmt.Println("\nTotal elapsed time: " + time.Now().Sub(start).String())
	forw[3].Debug_Print(ctx)
	// word, err:=forw[1].Get(ctx, []byte(strconv.Itoa(9)))
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println(string(word), word)
}
