package parser

import (
	//"bytes"
	"crypto/md5"
	"encoding/hex"
	"github.com/surgebase/porter2"
	"golang.org/x/net/html"
	"io/ioutil"
	"net/url"
	"regexp"
	"strings"
)

var stopWords = make(map[string]bool)

type Term struct {
	Content string
	Freq    map[string]uint32
	Pos     map[string][]float32
}

func Parse(doc *html.Node, baseURL string) (titleInfo Term, bodyInfo Term, fancyInfo map[string]Term, cleanFancy map[string][]string) {
	title, words, meta, fancy, fancyURLs := tokenize(doc, baseURL)
	// Clean terms in title and body
	cleanTitle := Laundry(title)
	cleanBody := Laundry(strings.Join(words, " "))
	cleanMeta := Laundry(strings.Join(meta, " "))
	cleanFancy = make(map[string][]string)
	for i, f := range fancy {
		urlHash := md5.Sum([]byte(fancyURLs[i]))
		urlHashString := hex.EncodeToString(urlHash[:])
		cleanFancy[urlHashString] = append(cleanFancy[urlHashString], Laundry(f)...)
	}

	// Get frequency and positions of each term
	// in title and body
	freqTitle, posTitle := getWordInfo(cleanTitle, cleanMeta)
	freqBody, posBody := getWordInfo(cleanBody, nil)
	titleInfo = Term{Content: title, Freq: freqTitle, Pos: posTitle}
	bodyInfo = Term{Freq: freqBody, Pos: posBody}
	fancyInfo = make(map[string]Term)
	for k, v := range cleanFancy {
		freqFancy, posFancy := getWordInfo(v, nil)
		fancyInfo[k] = Term{Freq: freqFancy, Pos: posFancy}
	}
	return
}

func tokenize(doc *html.Node, baseURL string) (title string,
	words, meta, fancy, fancyURLs []string) {

	//doc_, err := html.Parse(bytes.NewReader(doc))
	//if err != nil {
	//	panic(err)
	//}
	var f func(*html.Node, string)
	f = func(n *html.Node, baseURL string) {
		if n.Type == html.ElementNode {
			if n.Data == "title" {
				if n.FirstChild != nil {
					title = strings.TrimSpace(n.FirstChild.Data)
				}
			} else if n.Data == "meta" {
				var name string
				var content string
				for _, attr := range n.Attr {
					if attr.Key == "name" {
						name = attr.Val
					}
					if attr.Key == "content" {
						content = attr.Val
					}
				}
				if name == "description" || name == "keywords" || name == "author" {
					meta = append(meta, content)
				}
			}
		} else if n.Type == html.TextNode {
			tempD := n.Parent.Data
			cleaned := strings.TrimSpace(n.Data)
			if tempD != "title" && tempD != "script" && tempD != "style" && tempD != "noscript" && tempD != "iframe" && cleaned != "" {
				if tempD == "a" {
					for _, attr := range n.Parent.Attr {
						if attr.Key == "href" {
							urlRe := regexp.MustCompile("[^A-Za-z0-9-._~:/?#[]@!$&'()\\*\\+,;=]|\r?\n| ")
							/* Skip if no href or
							if href is anchor or
							if href is mail or script */
							if attr.Val == "" ||
								attr.Val[0] == '#' ||
								strings.HasPrefix(attr.Val, "javascript") ||
								strings.HasPrefix(attr.Val, "mailto") {
								break
							}

							thisURL := ""
							/* Make sure the URL ends without '/' */
							if strings.HasSuffix(attr.Val, "/") {
								thisURL = attr.Val[:len(attr.Val)-1]
							} else {
								thisURL = attr.Val
							}

							/* Ignore media files */
							isMedia := false
							mediaExs := []string{
								".mp3", ".pdf", ".png", ".jpg", ".mp4", ".avi",
								".zip", ".pptx", ".ppt", ".rar", ".doc", ".docx",
								".tar", ".gz", ".xz", ".bz", ".7z",
							}
							for _, ex := range mediaExs {
								if strings.HasSuffix(strings.ToLower(thisURL), ex) {
									isMedia = true
									break
								}
							}
							if isMedia {
								continue
							}

							if len(thisURL) == 0 {
								break
							}

							var tail string
							if len(thisURL) < 4 ||
								(thisURL[:4] != "http" && thisURL[:4] != "www.") {
								baseURLtype, e := url.Parse(baseURL)
								if e != nil {
									panic(e)
								}
								hn := baseURLtype.Hostname()
								sc := baseURLtype.Scheme

								if thisURL[0] != '/' {
									tail = urlRe.ReplaceAllString(baseURL+"/"+thisURL, "")
								} else {
									tail = urlRe.ReplaceAllString(sc+"://"+hn+thisURL, "")
								}
							} else {
								tail = urlRe.ReplaceAllString(thisURL, "")
							}
							fancyURLs = append(fancyURLs, tail)
							fancy = append(fancy, cleaned)
						}
						break
					}
				}
				words = append(words, cleaned)
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c, baseURL)
		}
	}
	f(doc, baseURL)

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

func getWordInfo(words []string, meta []string) (termFreq map[string]uint32, termPos map[string][]float32) {
	termFreq = make(map[string]uint32)
	termPos = make(map[string][]float32)
	for pos, word := range words {
		termPos[word] = append(termPos[word], float32(pos))
		termFreq[word] = termFreq[word] + 1
	}
	for _, word := range meta {
		termPos[word] = append(termPos[word], float32(-100))
		termFreq[word] = termFreq[word] + 1
	}
	return
}
