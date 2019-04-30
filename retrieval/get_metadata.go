package retrieval

import (
	"context"
	"math"
	//"bytes"
	"sync"
//	"io/ioutil"
	db "the-SearchEngine/database"
	"log"
	//"strings"
	//"golang.org/x/net/html"
//	"the-SearchEngine/indexer"
)

func computeFinalRank(ctx context.Context, docs <-chan Rank_result, forw []db.DB, queryLength int) <-chan Rank_combined {
	out := make(chan Rank_combined)
	go func() {
		for doc := range docs {
			// get doc metadata using future pattern for faster performance
			metadata := getDocInfo(ctx, doc.DocHash, forw)
			summary := getSummary(doc.DocHash)

			// get pagerank value
			var PR float64
			if tempVal, err := forw[3].Get(ctx, doc.DocHash); err != nil {
				panic(err)
			} else {
				PR = tempVal.(float64)
			}

			// get page magnitude for cossim normalisation
			var pageMagnitude map[string]float64
			if tempVal, err := forw[4].Get(ctx, doc.DocHash); err != nil {
				panic(err)
			} else {
				pageMagnitude = tempVal.(map[string]float64)
			}

			// compute final rank
			queryMagnitude := math.Sqrt(float64(queryLength))

			doc.BodyRank /= (pageMagnitude["body"] * queryMagnitude)
			doc.TitleRank /= (pageMagnitude["title"] * queryMagnitude)

			// retrieve result from future, assign ranking
			docMetaData := <-metadata
			docMetaData.PageRank = PR
			docMetaData.FinalRank = 0.3*PR + 0.4*doc.TitleRank + 0.3*doc.BodyRank
			docMetaData.Summary = <-summary
			log.Print(docMetaData.Summary)

			out <- docMetaData
		}
		close(out)
	}()
	return out
}

func getSummary(docHash string)  <-chan string{
	out := make(chan string)
	defer close(out)
	go func() {
		// read cached files
		//htmResp, err := ioutil.ReadFile(indexer.DocsDir+docHash)
		if true {
			log.Print("beenheredonethat")
			temp := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa"
			out <- temp
		} else {
/*
			doc, err := html.Parse(bytes.NewReader(htmResp))
			if err != nil {
				panic(err)
			}
			
			// extract text from html body
			var words []string
			var extractWord func(*html.Node)
			extractWord = func(n *html.Node) {
				if n.Type == html.TextNode{
					tempD := n.Parent.Data
					cleaned := strings.TrimSpace(n.Data)
					if tempD != "title" && tempD != "script" && tempD != "style" && tempD != "noscript" && tempD != "iframe" && tempD != "a" && tempD != "nav" && cleaned != "" {
						words = append(words, cleaned)
					}
				}
				for c := n.FirstChild; c != nil; c = c.NextSibling {
					extractWord(c)
				}
			}
			extractWord(doc)
			
			// pre-process words extracted
			words = strings.Fields(strings.Join(words, " "))
			
			if len(words) > 21 {
				var temp []string
				i := int(math.Ceil(float64(len(words)) / 2.0))
				temp = append(temp, "...")
				temp = append(temp, words[i-10:i+11]...)
				temp = append(temp, "...")
				out <- strings.Join(temp, " ")
			} else {
				words = append(words, "...")
				out <- strings.Join(words, " ")
			}
			log.Print("out")
*/
		}
	}()
	return out
}

func getDocInfo(ctx context.Context, docHash string, forw []db.DB) <-chan Rank_combined {
	out := make(chan Rank_combined, 1)

	go func() {
		var val db.DocInfo
		if tempVal, err := forw[1].Get(ctx, docHash); err != nil {
			panic(err)
		} else {
			val = tempVal.(db.DocInfo)
		}

		ret := resultFormat(val, 0, 0, "")

		parentChan := convertHashDocinfo(ctx, ret.Parents, forw)
		childrenChan := convertHashDocinfo(ctx, ret.Children, forw)
		wordmapChan := convertHashWords(ctx, ret.Words_mapping, forw)

		ret.Parents = <-parentChan
		ret.Children = <-childrenChan
		ret.Words_mapping = <-wordmapChan

		out <- ret
	}()
	return out
}

func convertHashDocinfo(ctx context.Context, docHashes []string, forw []db.DB) <-chan []string {
	out := make(chan []string, 1)

	// early stopping
	if docHashes == nil || len(docHashes) == 0 {
		out <- nil
		return out
	}

	go func() {
		// generate common input
		docHashInChan := genTermPipeline(docHashes)

		// fan-out to several getter
		numFanOut := len(docHashes)
		docOutChan := [](<-chan string){}
		for i := 0; i < numFanOut; i++ {
			docOutChan = append(docOutChan, retrieveUrl(ctx, docHashInChan, forw))
		}

		// fan-in result
		var resultUrl []string
		for docUrl := range fanInUrl(docOutChan) {
			resultUrl = append(resultUrl, docUrl)
		}

		out <- resultUrl
	}()
	return out
}

func retrieveUrl(ctx context.Context, docHashIn <-chan string, forw []db.DB) <-chan string {
	out := make(chan string, 1)
	go func() {
		for docHash := range docHashIn {
			var url string
			if val, err := forw[1].Get(ctx, docHash); err != nil {
				panic(err)
			} else {
				doc := val.(db.DocInfo)
				url = doc.Url.String()
			}

			out <- url
		}
		close(out)
	}()

	return out
}

func fanInUrl(urlIn []<-chan string) <-chan string {
	var wg sync.WaitGroup
	c := make(chan string)
	out := func(urls <-chan string) {
		defer wg.Done()
		for url := range urls {
			c <- url
		}
	}

	wg.Add(len(urlIn))
	for _, url := range urlIn {
		go out(url)
	}

	// close once all output goroutines are done
	go func() {
		wg.Wait()
		close(c)
	}()

	return c
}

func genWordPipeline(wordMap map[string]uint32) <-chan string {
	out := make(chan string, len(wordMap))
	defer close(out)
	for wordHash, _ := range wordMap {
		out <- wordHash
	}
	return out
}

func fanInWords(wordIn []<-chan map[string]string) <-chan map[string]string {
	var wg sync.WaitGroup
	c := make(chan map[string]string)
	out := func(words <-chan map[string]string) {
		defer wg.Done()
		for mapping := range words {
			for wordHash, wordStr := range mapping {
				c <- map[string]string{wordHash: wordStr}
			}
		}
	}

	wg.Add(len(wordIn))
	for _, word := range wordIn {
		go out(word)
	}

	// close once all output goroutines are done
	go func() {
		wg.Wait()
		close(c)
	}()

	return c
}

func retrieveWord(ctx context.Context, wordInChan <-chan string, forw []db.DB) <-chan map[string]string {
	out := make(chan map[string]string, 1)
	go func() {
		for word := range wordInChan {
			var wordStr string
			if val, err := forw[0].Get(ctx, word); err != nil {
				panic(err)
			} else {
				wordStr = val.(string)
			}

			out <- map[string]string{word: wordStr}
		}
		close(out)
	}()

	return out
}

func convertHashWords(ctx context.Context, wordMap map[string]uint32, forw []db.DB) <-chan map[string]uint32 {
	out := make(chan map[string]uint32, 1)

	// early stopping
	if wordMap == nil {
		out <- nil
		return out
	}

	go func() {
		// generate common channel for word input
		wordInChan := genWordPipeline(wordMap)

		// fan-out to multiple workers to get the word in string
		// word list is limited to 5
		numFanOut := len(wordMap)
		wordOutChan := [](<-chan map[string]string){}
		for i := 0; i < numFanOut; i++ {
			wordOutChan = append(wordOutChan, retrieveWord(ctx, wordInChan, forw))
		}

		// fan-in word hash mapping
		ret := make(map[string]uint32, len(wordMap))
		for hashToWord := range fanInWords(wordOutChan) {
			for wordHash, wordStr := range hashToWord {
				ret[wordStr] = wordMap[wordHash]
			}
		}

		out <- ret
		close(out)
	}()
	return out
}
