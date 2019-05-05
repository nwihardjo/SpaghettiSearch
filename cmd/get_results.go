package main

import (
	/*
		"context"
		"encoding/json"
	*/
	"fmt"
	//"github.com/apsdehal/go-logger"
	"os"
	/*
		"strconv"
		"strings"
		"github.com/nwihardjo/SpaghettiSearch/g/database"
		"time"
	*/)

func main() {
	// Init DB
	fmt.Println("============= THIS FILE IS OUTDATED AND WILL NOT RUN PROPERLY ============")
	fmt.Println("====== UPDATE THIS FILE FIRST TO INTEGRATE WITH THE NEW DB STRUCTURE =====")
	fmt.Println("============================= RETURNING ==================================")
	os.Exit(1)
	/*
		ctx, cancel := context.WithCancel(context.TODO())
		log, _ := logger.New("test", 1)
		inv, forw, _ := database.DB_init(ctx, log)
		for _, bdb_i := range inv {
			defer bdb_i.Close(ctx, cancel)
		}
		for _, bdb := range forw {
			defer bdb.Close(ctx, cancel)
		}

		// Output into a file
		f, err := os.Create("./spider_result.txt")
		if err != nil {
			panic(err)
		}
		defer f.Close()

		// Load all data containing DocInfo --> URL
		fin_dat, err := forw[3].Iterate(ctx)
		var final_data []database.DocInfo
		if err != nil {
			panic(err)
		}
		for _, kv := range fin_dat.KV {
			var tempDocInfo database.DocInfo
			err = json.Unmarshal(kv.Value, &tempDocInfo)
			if err != nil {
				panic(err)
			}
			// Remove unscraped data, present for the sake of printing out child url
			if tempDocInfo.Page_size == 0 {
				continue
			}
			final_data = append(final_data, tempDocInfo)
		}

		// Writing result into the file one data at each time
		outputSeparator := "------------------------------------------------------------ \n"
		for _, v := range final_data {
			lineThree := []string{v.Mod_date.Format(time.RFC1123), strconv.Itoa(int(v.Page_size))}
			// Iterate through the keywords and frequency
			wordFreq := []string{}
			for wordId, freq := range v.Words_mapping {
				word, err := forw[1].Get(ctx, []byte(strconv.Itoa(int(wordId))))
				if err != nil {
					panic(err)
				}
				wordFreq = append(wordFreq, string(word)+" "+strconv.Itoa(int(freq)))
			}

			// Iterate through the children of the URL
			childUrl := []string{}
			for _, child := range v.Children {
				var tempData database.DocInfo
				byteDocInfo, err := forw[3].Get(ctx, []byte(strconv.Itoa(int(child))))
				if err != nil {
					panic(err)
				}
				err = json.Unmarshal(byteDocInfo, &tempData)
				if err != nil {
					panic(err)
				}
				childUrl = append(childUrl, "Child "+tempData.Url.String())
			}

			// Append all info for a document into a formatted string to be written
			output := []string{strings.Join(v.Page_title, " "), v.Url.String(), strings.Join(lineThree, ", "), strings.Join(wordFreq, "; "), strings.Join(childUrl, " \n"), outputSeparator}
			_, err := f.WriteString(strings.Join(output, " \n"))
			if err != nil {
				panic(err)
			}
			f.Sync()
		}
		fmt.Println("Finished writing spider_result.txt")

		// word, err:=forw[1].Get(ctx, []byte(strconv.Itoa(9)))
		// if err != nil {
		// 	panic(err)
		// }
		// fmt.Println(string(word), word)
	*/
}
