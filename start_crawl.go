package main

import (
	"./crawler"
	"crypto/tls"
	"fmt"
	"github.com/eapache/channels"
	"net/http"
	"os"
	"sync"
	"time"
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
	maxThreadNum := 10
	visited := channels.NewInfiniteChannel()
	queue := channels.NewInfiniteChannel()
	var wg sync.WaitGroup

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
				go crawler.Crawl(idx, &wg, currentURL, client, queue)

			} else {
				os.Exit(1)
			}
		}

		/* Wait for all children to finish */
		wg.Wait()

		if queue.Len() <= 0 {
			break
		}
	}

	/* Close the visited and queue channels */
	visited.Close()
	queue.Close()

	fmt.Println("\nTotal elapsed time: " + time.Now().Sub(start).String())
}
