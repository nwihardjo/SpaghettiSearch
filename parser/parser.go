package parser

import (
	"bytes"
  "io/ioutil"
	"github.com/surgebase/porter2"
	"golang.org/x/net/html"
	"regexp"
	"strings"
)

var stopWords = make(map[string]bool)

type Term struct {
	Content string
  Freq map[string]uint32
  Pos map[string][]uint32
}

func Parse(doc []byte) (titleInfo Term, bodyInfo Term) {
  title, words := tokenize(doc)
  // Clean terms in title and body
  cleanTitle := Laundry(title)
  cleanBody := Laundry(strings.Join(words, " "))

  // Get frequency and positions of each term
  // in title and body
	freqTitle, posTitle := getWordInfo(cleanTitle)
	freqBody, posBody := getWordInfo(cleanBody)
	titleInfo = Term{Content: title, Freq: freqTitle, Pos: posTitle}
	bodyInfo = Term{Freq: freqBody, Pos: posBody}
  return
}

func tokenize(doc []byte) (title string, words []string) {
  var prevToken string
  //Tokenize document
  tokenizer := html.NewTokenizer(bytes.NewReader(doc))
  for {
    tokenType := tokenizer.Next()
    // end of file or html error
    if tokenType == html.ErrorToken {
      break
    }
    token := tokenizer.Token()
    switch tokenType {
    case html.StartTagToken:
      if token.Data == "title" {
        tokenizer.Next()
        title = strings.TrimSpace(tokenizer.Token().Data)
      }
      prevToken = token.Data
      break
    case html.TextToken:
      cleaned := strings.TrimSpace(token.Data)
      if prevToken != "script" && prevToken != "a" && prevToken != "style" && cleaned != "" {
        words = append(words, cleaned)
      }
      break
    }
  }
  return
}

func isStopWord(s string) (isStop bool) {
	// create stopWords map if its 0
	if len(stopWords) == 0 {
		// import stopword file
		content, err := ioutil.ReadFile("./indexer/stopwords.txt")
		if err != nil {
			panic(err)
		}
		wordString := strings.Split(string(content), "\n")
		for _, word := range wordString {
			stopWords[word] = true
		}
	}
	isStop = stopWords[s]
	return
}

func Laundry(s string) (c []string) {
	// remove all special characters
	regex := regexp.MustCompile("[^a-zA-Z0-9]")
	s = regex.ReplaceAllString(s, " ")
	// remove unnecessary spaces
	regex = regexp.MustCompile("[^\\s]+")
	words := regex.FindAllString(s, -1)
	// loop through each word and clean them ~laundry time~
	for _, word := range words {
		cleaned := strings.TrimSpace(strings.ToLower(word))
		cleaned = porter2.Stem(cleaned)
		if !isStopWord(cleaned) {
			c = append(c, cleaned)
		}
	}
	return
}

func getWordInfo(words []string) (termFreq map[string]uint32, termPos map[string][]uint32) {
	termFreq = make(map[string]uint32)
	termPos = make(map[string][]uint32)
	for pos, word := range words {
		termPos[word] = append(termPos[word], uint32(pos))
		termFreq[word] = termFreq[word] + 1
	}
	return
}
