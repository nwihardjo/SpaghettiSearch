package crawler

import (
	"fmt"
	"github.com/eapache/channels"
	"golang.org/x/net/html"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

func EnqueueChildren(n *html.Node, baseURL string, queue *channels.InfiniteChannel) {
	if n.Type == html.ElementNode && n.Data == "a" {
		for a := 0; a < len(n.Attr); a++ {
			if n.Attr[a].Key == "href" {

				/* Skip if no href or if href is anchor */
				if n.Attr[a].Val == "" || n.Attr[a].Val[0] == '#' {
					continue
				}

				/*
					If the href starts with '/', append this to baseURL
					Example:
						baseURL = "https://example.com"
						href = "/admin"
						nextURL = "https://example.com/admin"
				*/
				if n.Attr[a].Val[0] == '/' {
					if baseURL[len(baseURL)-1] == '/' {
						queue.In() <- baseURL[:len(baseURL)-1] + n.Attr[a].Val
					} else {
						queue.In() <- baseURL + n.Attr[a].Val
					}
				} else {
					queue.In() <- n.Attr[a].Val
				}

				break
			}
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		EnqueueChildren(c, baseURL, queue)
	}
}

func Crawl(idx int, wg *sync.WaitGroup, currentURL string, client *http.Client, queue *channels.InfiniteChannel) {
	defer wg.Done()
	defer resp.Body.Close()

	innerStart := time.Now()
	resp, err := client.Get(currentURL)
	fmt.Println("Goroutine id " + strconv.Itoa(idx) + " visited " + currentURL + " (elapsed time: " + time.Now().Sub(innerStart).String() + ")")

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Print("Last Modified: ")
	lm := resp.Header.Get("Last-Modified")
	if lm == "" {
		fmt.Println("None")
	} else {
		fmt.Println(lm)
	}
	// Send resp, url, and last modified to indexer here
	// (non-blocking)
	doc, err := html.Parse(resp.Body)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	EnqueueChildren(doc, currentURL, queue)
}
