package main

import (
	"crypto/tls"
	"fmt"
	"github.com/eapache/channels"
	"golang.org/x/net/html"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

func main(){
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

	queue.In() <- startURL

	var f func(*html.Node, string)
	f = func(n *html.Node, baseURL string) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for a := 0; a < len(n.Attr); a++ {
				if n.Attr[a].Key == "href" {
					if n.Attr[a].Val == "" || n.Attr[a].Val[0] == '#' {
						continue
					}
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
			f(c, baseURL)
		}
	}

	var crawl func(int, *sync.WaitGroup, string)
	crawl = func(idx int, wg *sync.WaitGroup, currentURL string) {
		defer wg.Done()

		innerStart := time.Now()
		resp, err := client.Get(currentURL)
		fmt.Println("Goroutine id " + strconv.Itoa(idx) + " visited " + currentURL + " (elapsed time: " + time.Now().Sub(innerStart).String() + ")")

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		doc, err := html.Parse(resp.Body)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		f(doc, currentURL)

		resp.Body.Close()
	}

	var wg sync.WaitGroup

	for visited.Len() < numOfPages {
		for idx := 0; queue.Len() > 0 && idx < maxThreadNum && visited.Len() < numOfPages; idx++ {
			if currentURL, ok := (<-queue.Out()).(string); ok {
				flag := false
				var temp []string
				for i := 0; i < visited.Len(); i++ {
					v, ok2 := (<-visited.Out()).(string)
					if !ok2 {
						break
					}
					temp = append(temp, v)
					if v == currentURL {
						flag = true
					}
				}
				for _, t := range temp {
					visited.In() <- t
				}
				if flag {
					idx--
					continue
				}
				visited.In() <- currentURL

				wg.Add(1)
				go crawl(idx, &wg, currentURL)

			} else {
				os.Exit(1)
			}
		}

		wg.Wait()

		if queue.Len() <= 0 {
			break
		}
	}

	visited.Close()
	queue.Close()

	fmt.Println("\nTotal elapsed time: " + time.Now().Sub(start).String())
}
