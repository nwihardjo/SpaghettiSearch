package main


import (
	"fmt"
	"net/http"
	"os"
	"golang.org/x/net/html"
)

func main() {
	fmt.Println("Crawler started...")

	var visited []string

	resp, err := http.Get("https://www.cse.ust.hk/")
	visited = append(visited, "https://www.cse.ust.hk/")

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer resp.Body.Close()

	doc, err := html.Parse(resp.Body)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for a := 0; a < len(n.Attr); a++ {
				if n.Attr[a].Key == "href" {
					if string(n.Attr[a].Val[0]) == "#" {
						continue
					}
					if string(n.Attr[a].Val[0]) == "/" {
						fmt.Println("https://www.cse.ust.hk" + n.Attr[a].Val)
					} else {
						fmt.Println(n.Attr[a].Val)
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}

	f(doc)
	fmt.Println("[" + visited[0] + "]")
}
