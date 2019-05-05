package retrieval

import (
	"bytes"
	"context"
	db "github.com/nwihardjo/SpaghettiSearch/database"
	"github.com/nwihardjo/SpaghettiSearch/indexer"
	"golang.org/x/net/html"
	"io/ioutil"
	"math"
	"regexp"
	"strings"
	"sync"
)

func computeFinalRank(ctx context.Context, docs <-chan Rank_result, forw []db.DB, queryLength int, query string, phrases []string) <-chan Rank_combined {
	out := make(chan Rank_combined, len(docs))
	defer close(out)
	var wg sync.WaitGroup

	for doc := range docs {
		wg.Add(1)
		go func(doc Rank_result) {
			defer wg.Done()

			// get doc metadata using future pattern for faster performance
			metadata := getDocInfo(ctx, doc.DocHash, forw)
			summary := getSummary(doc.DocHash, query, phrases)

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

			docMetaData := <-metadata

			doc.BodyRank /= (pageMagnitude["body"] * queryMagnitude)
			doc.TitleRank /= (pageMagnitude["title"] * queryMagnitude)

			// make rank to be 0 if division by zero occurs because it hasn't been indexed
			if math.IsNaN(doc.BodyRank) {
				doc.BodyRank = 0
			}
			if math.IsNaN(doc.TitleRank) {
				doc.TitleRank = 0
			}

			docMetaData.PageRank = PR
			docMetaData.FinalRank = (0.3*PR + 0.4*doc.TitleRank + 0.3*doc.BodyRank) * 100.0
			docMetaData.Summary = <-summary

			out <- docMetaData
		}(doc)
	}
	wg.Wait()
	return out
}

func getSummary(docHash, query string, phrases []string) <-chan string {
	out := make(chan string, 1)
	go func() {
		queryTokenised := strings.Fields(strings.Replace(strings.ToLower(query), "\"", "", -1))

		// read cached files
		htmResp, err := ioutil.ReadFile(indexer.DocsDir + docHash)
		if err != nil {
			out <- ""
		} else {
			doc, err := html.Parse(bytes.NewReader(htmResp))
			if err != nil {
				panic(err)
			}

			// extract text from html body
			var words []string
			var extractWord func(*html.Node)
			extractWord = func(n *html.Node) {
				if n.Type == html.ElementNode {
					tempD := n.Data
					if !(tempD != "title" && tempD != "script" && tempD != "style" && tempD != "noscript" && tempD != "iframe" && tempD != "a" && tempD != "nav") {
						for n.FirstChild != nil {
							n.RemoveChild(n.FirstChild)
						}
					}
				} else if n.Type == html.TextNode {
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

			// dynamic summary, if first query present in the database
			reg, err := regexp.Compile("[^a-zA-Z0-9]+")

			for i := 0; i < len(words); i++ {
				wordCleaned := strings.ToLower(reg.ReplaceAllString(words[i], ""))
				isMatch := false
				for j := 0; j < len(phrases); j++ {
					tempPhrase := strings.Fields(phrases[j])
					allMatch := true
					for k := 0; k < len(tempPhrase); k++ {
						word2 := strings.ToLower(reg.ReplaceAllString(tempPhrase[k], ""))
						if i+k >= len(words) {
							allMatch = false
							break
						}
						wordCleaned2 := strings.ToLower(reg.ReplaceAllString(words[i+k], ""))
						if wordCleaned2 != word2 {
							allMatch = false
							break
						}
					}
					if allMatch {
						isMatch = true
						break
					}
				}
				if !isMatch {
					for j := 0; j < len(queryTokenised); j++ {
						if wordCleaned == strings.ToLower(reg.ReplaceAllString(queryTokenised[j], "")) {
							isMatch = true
							break
						}
					}
				}

				if isMatch {
					temp := make([]string, 0, 20)
					diff := 0

					if (i - 10) < 0 {
						diff = 20 - i
						temp = append(temp, words[:i]...)
					} else {
						temp = append(temp, "...")
						temp = append(temp, words[i-10:i]...)
					}

					if diff == 0 {
						if (i + 10) <= len(words) {
							temp = append(temp, words[i:i+10]...)
							temp = append(temp, "...")
							out <- strings.Join(temp, " ")
							return
						} else {
							temp = append(temp, words[i:]...)
							out <- strings.Join(temp, " ")
							return
						}
					} else {
						if (i + diff) <= len(words) {
							temp = append(temp, words[i:i+diff]...)
							temp = append(temp, "...")
							out <- strings.Join(temp, " ")
							return
						} else {
							temp = append(temp, words[i:]...)
							out <- strings.Join(temp, " ")
							return
						}
					}
				}
			}

			// static summary
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
	out := make(chan string, len(docHashIn))
	defer close(out)
	var wg sync.WaitGroup

	for docHash := range docHashIn {
		wg.Add(1)
		go func(docHash string) {
			defer wg.Done()

			var url string
			if val, err := forw[1].Get(ctx, docHash); err != nil {
				panic(err)
			} else {
				doc := val.(db.DocInfo)
				url = doc.Url.String()
			}

			out <- url
		}(docHash)
	}

	wg.Wait()
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
	out := make(chan map[string]string, len(wordInChan))
	defer close(out)
	var wg sync.WaitGroup

	for word := range wordInChan {
		wg.Add(1)
		go func(word string) {
			defer wg.Done()

			var wordStr string
			if val, err := forw[0].Get(ctx, word); err != nil {
				panic(err)
			} else {
				wordStr = val.(string)
			}

			out <- map[string]string{word: wordStr}
		}(word)
	}

	wg.Wait()
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
