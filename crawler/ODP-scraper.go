package crawler

import (
	"bytes"
	"context"
	"fmt"
	"github.com/gocolly/colly"
	db "github.com/nwihardjo/SpaghettiSearch/database"
	"github.com/nwihardjo/SpaghettiSearch/parser"
	"golang.org/x/net/html"
	"net/url"
	"strings"
	"sync"
	"time"
)

type scrapedData struct {
	Category string
	Values   map[string]uint32
	NumPages int
}

func genTopic(listTopic []*url.URL) <-chan *url.URL {
	out := make(chan *url.URL, len(listTopic))
	defer close(out)
	for i := 0; i < len(listTopic); i++ {
		out <- listTopic[i]
	}
	return out
}

func fanInData(dataIn []<-chan *scrapedData) <-chan *scrapedData {
	var wg sync.WaitGroup
	c := make(chan *scrapedData)
	out := func(datdat <-chan *scrapedData) {
		defer wg.Done()
		for dat := range datdat {
			c <- dat
		}
	}

	wg.Add(len(dataIn))
	for _, data := range dataIn {
		go out(data)
	}

	go func() {
		wg.Wait()
		close(c)
	}()

	return c
}

func ParseODP(ctx context.Context, inv []db.DB, forw []db.DB) {
	timer := time.Now()
	var collector []*scrapedData

	c := colly.NewCollector(
		colly.MaxDepth(1),
	)

	// parse the top ODP topic
	c.OnHTML("#triple", func(e *colly.HTMLElement) {
		var listTopic []*url.URL
		e.ForEach("li", func(_ int, el *colly.HTMLElement) {
			if link, found := el.DOM.Find("a[href]:nth-of-type(1)").Attr("href"); found {
				u, err := url.Parse(link)
				if err != nil {
					panic(err)
				}
				listTopic = append(listTopic, u)
				// parseTopic(u)
			}
		})

		// generate commmon channel
		topicIn := genTopic(listTopic)

		// fan-out each topic to go routine
		topicOut := [](<-chan *scrapedData){}
		for i := 0; i < len(listTopic); i++ {
			topicOut = append(topicOut, parseTopic(topicIn))
		}

		// fan-in the parsed topic with its data
		for data := range fanInData(topicOut) {
			collector = append(collector, data)
		}

	})

	c.Visit("http://odp.org/")
	fmt.Println("\nTime to completely crawl ODP: ", time.Since(timer))
	timer = time.Now()

	bw_forw := forw[5].BatchWrite_init(ctx)
	defer bw_forw.Cancel(ctx)

	// aggregate scraped ODP data
	// final maps each word to a map of category to their frequency
	final := make(map[string]map[string]uint32)
	for _, data := range collector {
		metadata := map[string]float64{
			"numPages":  float64(data.NumPages),
			"wordCount": float64(len(data.Values)),
		}
		if err := bw_forw.BatchSet(ctx, data.Category, metadata); err != nil {
			panic(err)
		}

		for keyword, freq := range data.Values {
			if v, ok := final[keyword]; ok {
				v[data.Category] = freq
			} else {
				temp := make(map[string]uint32)
				temp[data.Category] = freq
				final[keyword] = temp
			}
		}
	}

	// batch write the number of pages contained in each category
	if err := bw_forw.Flush(ctx); err != nil {
		panic(err)
	}

	bw := inv[2].BatchWrite_init(ctx)
	defer bw.Cancel(ctx)

	// write aggregated data to db
	for k, v := range final {
		if err := bw.BatchSet(ctx, k, v); err != nil {
			panic(err)
		}
	}
	if err := bw.Flush(ctx); err != nil {
		panic(err)
	}

	fmt.Println("\nTime to put it into db: ", time.Since(timer))
}

func parseTopic(u <-chan *url.URL) <-chan *scrapedData {
	out := make(chan *scrapedData, len(u))
	defer close(out)
	var wg sync.WaitGroup

	for topic := range u {
		wg.Add(1)
		go func(u *url.URL) {
			defer wg.Done()

			c := colly.NewCollector(
				colly.Async(true),
			)

			// Limit the maximum parallelism to 300, necessary if goroutines
			// are dynamically created to control the limit of simultaneous requests
			c.Limit(&colly.LimitRule{
				DomainGlob:  "*",
				Parallelism: 100,
			})

			numPages := 0
			vectorTerm := newMapSync()

			// crawl subcategory
			c.OnHTML("html body div.container ul#triple", func(subCategories *colly.HTMLElement) {
				if subCategories.Request.URL.Host == u.Host {
					subCategories.ForEach("li a[href]", func(_ int, subCategory *colly.HTMLElement) {
						// avoid webpage crawled from different topic
						link := subCategory.Attr("href")
						if strings.HasPrefix(link, u.Path) {
							subCategory.Request.Visit(u.Scheme + "://" + u.Host + link)
						}
					})
				}
			})

			// crawl webpage resources
			c.OnHTML("html body div.container ul", func(listEntry *colly.HTMLElement) {
				if listEntry.Request.URL.Host == u.Host {
					listEntry.ForEach("li.listings h4 a[href]", func(_ int, entry *colly.HTMLElement) {
						entry.Request.Visit(entry.Attr("href"))
					})
				}
			})

			// scrape resource
			c.OnResponse(func(r *colly.Response) {
				if r.Request.URL.Host != u.Host {
					htmlReader := bytes.NewReader(r.Body)
					doc, err := html.Parse(htmlReader)
					if err != nil {
						panic(err)
					}

					titleInfo, bodyInfo, _, _ := parser.Parse(doc, r.Request.URL.String())

					// local aggregation
					for k, v := range titleInfo.Freq {
						bodyInfo.Freq[k] += v
					}
					// global aggregation
					for k, v := range titleInfo.Freq {
						vectorTerm.Add(k, v)
					}
				}
			})

			c.OnRequest(func(r *colly.Request) {
				numPages += 1
				if r.URL.Host != u.Host {
					//fmt.Println("DEBUG PARSING RESOURCE", r.URL.String())
				}
				fmt.Println("Visiting #", numPages, ": ", r.URL.String())
			})

			c.Visit(u.String())
			c.Wait()

			ret := &scrapedData{
				Category: strings.Replace(u.Path, "/", "", -1),
				Values:   vectorTerm.Values,
				NumPages: numPages,
			}

			out <- ret
		}(topic)
	}

	wg.Wait()
	return out
}
