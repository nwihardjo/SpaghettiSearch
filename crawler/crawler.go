package crawler

import (
	"fmt"
	"github.com/eapache/channels"
	"golang.org/x/net/html"
	"io/ioutil"
	"../indexer"
	"../database"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
	"bytes"
)

func EnqueueChildren(n *html.Node, baseURL string, queue *channels.InfiniteChannel, children *channels.InfiniteChannel) {
	if n.Type == html.ElementNode && n.Data == "a" {
		for a := 0; a < len(n.Attr); a++ {
			if n.Attr[a].Key == "href" {

				/* Skip if no href or if href is anchor */
				if n.Attr[a].Val == "" || n.Attr[a].Val[0] == '#' {
					continue
				}

				/* Make sure the URL ends without '/' */
				if n.Attr[a].Val[len(n.Attr[a].Val)-1] == '/' {
					n.Attr[a].Val = n.Attr[a].Val[:len(n.Attr[a].Val)-1]
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
						queue.In() <- []string{baseURL, baseURL[:len(baseURL)-1] + n.Attr[a].Val}
						children.In() <- baseURL[:len(baseURL)-1] + n.Attr[a].Val
					} else {
						queue.In() <- []string{baseURL, baseURL + n.Attr[a].Val}
						children.In() <- baseURL + n.Attr[a].Val
					}
				} else {
					queue.In() <- []string{baseURL, n.Attr[a].Val}
					children.In() <- n.Attr[a].Val
				}

				break
			}
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		EnqueueChildren(c, baseURL, queue, children)
	}
}

func Crawl(idx int, wg *sync.WaitGroup, parentURL string,
	currentURL string, client *http.Client,
	queue *channels.InfiniteChannel, mutex *sync.Mutex,
	inv []database.DB_Inverted, forw []database.DB) {

	defer wg.Done()

	innerStart := time.Now()
	resp, err := client.Get(currentURL)
	fmt.Println("Goroutine id " + strconv.Itoa(idx) + " visited " + currentURL + " (elapsed time: " + time.Now().Sub(innerStart).String() + ")")

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Print("Last Modified: ")
	lms := resp.Header.Get("Last-Modified")
	lm := time.Now().In(time.UTC)
	if lms != "" {
		lm, _ = time.Parse(time.RFC1123, lms)
		lm = lm.In(time.UTC)
	}
	fmt.Println(lm.String())

	htmlData, er := ioutil.ReadAll(resp.Body)
	if er != nil {
		panic(er)
		os.Exit(1)
	}
	htmlReader := bytes.NewReader(htmlData)

	doc, err := html.Parse(htmlReader)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	children := channels.NewInfiniteChannel()

	EnqueueChildren(doc, currentURL, queue, children)

	// Send resp, url, and last modified to indexer here
	// (non-blocking)

	var childs []string
	for i := 0; i < children.Len(); i++ {
		s, ok := (<-children.Out()).(string)
		if !ok {
			break
		}
		childs = append(childs, s)
	}

	indexer.Index(htmlData, currentURL, lm, mutex, inv, forw, parentURL, childs)

	children.Close()
	resp.Body.Close()
}
