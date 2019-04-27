package crawler

import (
	"bytes"
	"fmt"
	"github.com/eapache/channels"
	"golang.org/x/net/html"
	"golang.org/x/sync/semaphore"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"the-SearchEngine/database"
	"the-SearchEngine/indexer"
	"time"
)

func EnqueueChildren(n *html.Node, baseURL string, queue *channels.InfiniteChannel, children map[string]bool) {
	if n.Type == html.ElementNode && n.Data == "a" {
		for a := 0; a < len(n.Attr); a++ {
			if n.Attr[a].Key == "href" {
				urlRe := regexp.MustCompile("[^A-Za-z0-9-._~:/?#[]@!$&'()\\*\\+,;=]|\r?\n| ")

				/* Skip if no href or if href is anchor */
				if n.Attr[a].Val == "" ||
					n.Attr[a].Val[0] == '#' ||
					strings.HasPrefix(n.Attr[a].Val, "javascript") ||
					strings.HasPrefix(n.Attr[a].Val, "mailto") {
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
						head := urlRe.ReplaceAllString(baseURL, "")
						tail := urlRe.ReplaceAllString(baseURL+"/"+thisURL, "")
						queue.In() <- []string{head, tail}
						children[tail] = true
					} else {
						head := urlRe.ReplaceAllString(baseURL, "")
						tail := urlRe.ReplaceAllString(baseURL+thisURL, "")
						queue.In() <- []string{head, tail}
						children[tail] = true
					}
				} else {
					head := urlRe.ReplaceAllString(baseURL, "")
					tail := urlRe.ReplaceAllString(thisURL, "")
					queue.In() <- []string{head, tail}
					children[tail] = true
				}

				break
			}
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		EnqueueChildren(c, baseURL, queue, children)
	}
}

func Crawl(sem *semaphore.Weighted, parentURL string,
	currentURL string, errorsChannel *channels.InfiniteChannel, client *http.Client,
	lock2 *sync.RWMutex, queue *channels.InfiniteChannel, mutex *sync.Mutex,
	inv []database.DB, forw []database.DB) {

	defer sem.Release(1)

	innerStart := time.Now()
	resp, err := client.Get(currentURL)
	fmt.Println("Visited " + currentURL + " (elapsed time: " + time.Now().Sub(innerStart).String() + ")")

	if err != nil {
		errorsChannel.In() <- currentURL
		fmt.Println(err)
		return
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

	children := make(map[string]bool)

	EnqueueChildren(doc, currentURL, queue, children)

	var childsArr []string
	for k, _ := range children {
		childsArr = append(childsArr, k)
	}

	indexer.Index(htmlData, currentURL, lock2, lm, ps, mutex, inv, forw, parentURL, childsArr)

	resp.Body.Close()
}
