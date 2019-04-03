package crawler

import (
	"bytes"
	"fmt"
	"github.com/eapache/channels"
	"golang.org/x/net/html"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"the-SearchEngine/database"
	"the-SearchEngine/indexer"
	"time"
)

func EnqueueChildren(n *html.Node, baseURL string, queue *channels.InfiniteChannel, children *channels.InfiniteChannel) {
	if n.Type == html.ElementNode && n.Data == "a" {
		for a := 0; a < len(n.Attr); a++ {
			if n.Attr[a].Key == "href" {

				/* Skip if no href or if href is anchor */
				if n.Attr[a].Val == "" ||
					n.Attr[a].Val[0] == '#' ||
					(len(n.Attr[a].Val) >= 10 && n.Attr[a].Val[:10] == "javascript") ||
					(len(n.Attr[a].Val) >= 6 && n.Attr[a].Val[:6] == "mailto") {
					continue
				}

				thisURL := ""
				/* Make sure the URL ends without '/' */
				if n.Attr[a].Val[len(n.Attr[a].Val)-1] == '/' {
					thisURL = n.Attr[a].Val[:len(n.Attr[a].Val)-1]
				} else {
					thisURL = n.Attr[a].Val
				}

				/*
					If the href does not start with 'http' or 'www',
					append this to baseURL
					Example:
						baseURL = "https://example.com"
						href = "/admin"
						nextURL = "https://example.com/admin"
				*/
				if len(thisURL) == 0 {
					continue
				}
				if len(thisURL) < 4 ||
					(thisURL[:4] != "http" && thisURL[:4] != "www.") {

					if thisURL[0] != '/' {
						queue.In() <- []string{baseURL, baseURL + "/" + thisURL}
						children.In() <- baseURL + "/" + thisURL
					} else {
						queue.In() <- []string{baseURL, baseURL + thisURL}
						children.In() <- baseURL + thisURL
					}
				} else {
					queue.In() <- []string{baseURL, thisURL}
					children.In() <- thisURL
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
	ps := resp.Header.Get("Content-Length")
	lms := resp.Header.Get("Last-Modified")
	lm := time.Now().In(time.UTC)
	if lms != "" {
		lm, _ = time.Parse(time.RFC1123, lms)
		lm = lm.In(time.UTC)
	}
	fmt.Println(lm.String())
	fmt.Print("File Size: ")
	if ps == "" {
		fmt.Println("<unknown>")
	} else {
		fmt.Println(ps)
	}

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

	childs := make(map[string]int)
	childrenLen := children.Len()
	for i := 0; i < childrenLen; i++ {
		s, ok := (<-children.Out()).(string)
		if !ok {
			break
		}
		childs[s] += 1
	}
	var childsArr []string
	for k, _ := range childs {
		childsArr = append(childsArr, k)
	}

	indexer.Index(htmlData, currentURL, lm, ps, mutex, inv, forw, parentURL, childsArr)

	children.Close()
	resp.Body.Close()
}
