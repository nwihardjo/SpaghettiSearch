package main


import (
	"fmt"
	"net/http"
	"os"
	"golang.org/x/net/html"
	"github.com/eapache/channels"
	"crypto/tls"
)

func main() {
	fmt.Println("Crawler started...")

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify : true},
	}
	client := &http.Client{Transport: tr}

	startURL := "https://www.cse.ust.hk/"
	numOfPages := 10
	visited := make(chan string, numOfPages)
	queue := channels.NewResizableChannel()

	queue.In() <- startURL

	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for a := 0; a < len(n.Attr); a++ {
				if n.Attr[a].Key == "href" {
					if n.Attr[a].Val == "" || string(n.Attr[a].Val[0]) == "#" {
						continue
					}
					if string(n.Attr[a].Val[0]) == "/" {
						queue.Resize(channels.BufferCap(queue.Len() + 1))
						queue.In() <- "https://www.cse.ust.hk" + string(n.Attr[a].Val)
					} else {
						queue.Resize(channels.BufferCap(queue.Len() + 1))
						queue.In() <- string(n.Attr[a].Val)
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}

	for ; len(visited) < numOfPages && queue.Len() > 0; {
		if currentURL, ok := (<-queue.Out()).(string); ok {
			flag := false
			var temp []string
			close(visited)
			for v := range visited {
				temp = append(temp, v)
				if(v == currentURL) {
					flag = true
				}
			}
			visited = make(chan string, numOfPages)
			for _, t := range temp {
				visited <- t
			}
			if flag {
				continue
			}
			fmt.Println("Visiting " + currentURL)
			resp, err := client.Get(currentURL)
			visited <- currentURL

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			doc, err := html.Parse(resp.Body)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			f(doc)

			resp.Body.Close()
		} else {
			os.Exit(1)
		}
	}

	close(visited)

	for v := range visited {
		fmt.Println(v)
	}

	queue.Close()
}
