package main

import (
	"./database"
	"context"
	"encoding/json"
	"fmt"
	"github.com/apsdehal/go-logger"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	/*
	        fmt.Println("Generating spider_result.txt...")
		// populatin entry, TODO: delete upon submission
	        a1, _ := url.Parse("http://www.google.com")
	        b1, _ := url.Parse("http://www.fb.com")
	        c1, _ := url.Parse("http://github.com")
	        key := []*url.URL{a1, b1, c1}

	        t := make(map[uint32]uint32)
		t[1]=10
	        t[2]=20
	        t[3]=30
		tv := []string{"apple", "orange", "juice"}

	        a := database.DocInfo {
	                DocId: 1,
	                Page_title: []string{"asd","sdf"},
	                Mod_date: time.Now(),
	                Page_size: 23,
	                Children: []uint16{2,3},
	                Parents: []uint16{1},
	                Words_mapping: t,
	        }

	        b := database.DocInfo {
	                DocId: 2,
	                Page_title: []string{"aasd","sadf"},
	                Mod_date: time.Now(),
	                Page_size: 233,
	                Children: []uint16{3},
	                Parents: []uint16{1},
	                Words_mapping: t,
	        }

	        c := database.DocInfo {
	                DocId: 3,
	                Page_title: []string{"aasdsd","asdsdf"},
	                Mod_date: time.Now(),
	                Page_size: 23123,
	                Children: []uint16{1},
	                Parents: []uint16{2, 1},
	                Words_mapping: t,
	        }


	        value := []database.DocInfo{a, b, c}
	*/

	ctx, _ := context.WithCancel(context.TODO())
	log, err := logger.New("spider_result.txt generation", 1)
	if err != nil {
		panic(err)
	}

	// Initiating connection with database
	_, forw, err := database.DB_init(ctx, log)
	if err != nil {
		panic(err)
	}

	/*
		forw[1].DropTable(ctx)
		forw[2].DropTable(ctx)
		forw[3].DropTable(ctx)

		//populating db, TODO: delete upon pushing
		for k, v := range key {
			temp, _ := v.MarshalBinary()
			tempv, _ := json.Marshal(value[k])
			forw[2].Set(ctx, temp, tempv)
			forw[3].Set(ctx, []byte(strconv.Itoa(int(value[k].DocId))), temp)
		}
		for k, _ := range t {
			forw[1].Set(ctx, []byte(strconv.Itoa(int(k))), []byte(tv[k-1]))
		}
		forw[1].Debug_Print(ctx)
		forw[2].Debug_Print(ctx)
		forw[3].Debug_Print(ctx)
	*/

	f, err := os.Create("./spider_result.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Load all data from forw[2] table which contain URL --> DocInfo
	data, err := forw[2].Iterate(ctx)
	if err != nil {
		panic(err)
	}

	forward2Data := make(map[url.URL]database.DocInfo)
	for _, kv := range data.KV {
		tempURL := &url.URL{}
		if err = tempURL.UnmarshalBinary(kv.Key); err != nil {
			panic(err)
		}

		var tempDocInfo database.DocInfo
		err = json.Unmarshal(kv.Value, &tempDocInfo)
		if err != nil {
			panic(err)
		}
		forward2Data[*tempURL] = tempDocInfo
	}

	// Iterate and output each URL / document
	outputSeparator := "-------------------------------------------------------------- \n"
	// forward2Data is already in DocInfo
	for k, v := range forward2Data {
		lineThree := []string{v.Mod_date.Format(time.RFC1123), strconv.Itoa(int(v.Page_size))}

		/* Iterate through the keywords and frequency */
		wordFreq := []string{}
		for wordId, freq := range v.Words_mapping {
			word, err := forw[1].Get(ctx, []byte(strconv.Itoa(int(wordId))))
			if err != nil {
        fmt.Println(wordId, freq)
				panic(err)
			}
			wordFreq = append(wordFreq, string(word)+" "+strconv.Itoa(int(freq)))
		}

		// Iterate through the children to find the URL
		childUrl := []string{}
		for _, v := range v.Children {
			tempUrl := &url.URL{}
			byteUrl, err := forw[3].Get(ctx, []byte(strconv.Itoa(int(v))))
			if err != nil {
				panic(err)
			}
			err = tempUrl.UnmarshalBinary(byteUrl)
			if err != nil {
				panic(err)
			}
			childUrl = append(childUrl, "Child "+tempUrl.String())
		}

		// Append all info for a particular document into one string to be written to file
		output := []string{strings.Join(v.Page_title, " "), k.String(), strings.Join(lineThree, ", "), strings.Join(wordFreq, "; "), strings.Join(childUrl, " \n"), outputSeparator}
		_, err := f.WriteString(strings.Join(output, " \n"))
		if err != nil {
			panic(err)
		}
		f.Sync()
	}

	fmt.Println("finished writing spider_result.txt")
}
