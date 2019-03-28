package database

//package main

import (
	//"context"
	"encoding/json"
	//"fmt"
	//"github.com/apsdehal/go-logger"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type InvKeyword_value struct {
	DocId int32   `json:"DocId"`
	Pos   []int32 `json:"Pos"`
}

type InvKeyword_values []InvKeyword_value

type URL_value struct {
	DocId         int32           `json:"DocId"`
	Mod_date      time.Time       `json:"Mod_date"`
	Page_size     int32           `json:"Page_size"`
	Children      []int32         `json:"Childrens"`
	Parents       []int32         `json:"Parents"`
	Words_mapping map[int32]int32 `json:"Words_mapping"`
	//mapping for wordId to wordFrequency
}

//TODO: perform benchmarking between using struct and space in between URL and location
type DocId_value struct {
	URL     url.URL `json:"URL"`
	FileLoc string  `json:"FileLoc"`
}

func (u URL_value) MarshalJSON() ([]byte, error) {
	basicURL_value := struct {
		DocId         int32           `json:"DocId"`
		Mod_date      string          `json:"Mod_date"`
		Page_size     int32           `json:"Page_size"`
		Children      []int32         `json:"Childrens"`
		Parents       []int32         `json:"Parents"`
		Words_mapping map[int32]int32 `json:"Words_mapping"`
	}{u.DocId, u.Mod_date.Format(time.RFC1123), u.Page_size, u.Children, u.Parents, u.Words_mapping}

	return json.Marshal(basicURL_value)
}

func (u *URL_value) UnmarshalJSON(j []byte) error {
	var rawStrings map[string]interface{}

	err := json.Unmarshal(j, &rawStrings)
	if err != nil {
		return err
	}

	for k, v := range rawStrings {
		if strings.ToLower(k) == "docid" {
			/* ParseInt check whether string can be mapped into int 32-bit
			   but still return int 64-bit. Further casting is then needed */
			u.DocId = int32(v.(float64))
		} else if strings.ToLower(k) == "mod_date" {
			if u.Mod_date, err = time.Parse(time.RFC1123, v.(string)); err != nil {
				return err
			}
		} else if strings.ToLower(k) == "page_size" {
			u.Page_size = int32(v.(float64))
		} else if strings.ToLower(k) == "children" {
			u.Children = make([]int32, len(v.([]interface{})))
			for k_, v_ := range v.([]interface{}) {
				u.Children[k_] = int32(v_.(float64))
			}
		} else if strings.ToLower(k) == "parents" {
			u.Parents = make([]int32, len(v.([]interface{})))
			for k_, v_ := range v.([]interface{}) {
				u.Parents[k_] = int32(v_.(float64))
			}
		} else if strings.ToLower(k) == "words_mapping" {
			u.Words_mapping = make(map[int32]int32)
			for k_, v_ := range v.(map[string]interface{}) {
				str, _ := strconv.ParseInt(k_, 0, 32)
				u.Words_mapping[int32(str)] = int32(v_.(float64))
			}
		}
	}

	return nil
}

func (d DocId_value) MarshalJSON() ([]byte, error) {
	basicDocId_value := struct {
		URL     string `json:"URL"`
		FileLoc string `json:"FileLoc"`
	}{d.URL.String(), d.FileLoc}

	return json.Marshal(basicDocId_value)
}

func (d *DocId_value) UnmarshalJSON(j []byte) error {
	var rawStrings map[string]string

	err := json.Unmarshal(j, &rawStrings)
	if err != nil {
		return err
	}

	for k, v := range rawStrings {
		if strings.ToLower(k) == "url" {
			u, err := url.Parse(v)
			if err != nil {
				return err
			}
			d.URL = *u
		} else if strings.ToLower(k) == "fileloc" {
			d.FileLoc = v
		}
	}

	return nil
}

/*
func main() {
	Name := make(map[int32]int32)
	Name[0] = 0
	Name[1] = 12
	Name[2] = 23
	Name[3] = 40

	temp1 := URL_value{
		DocId: 1,
		Mod_date: time.Now(),
		Page_size: 1,
		Children: []int32{1,2,3},
		Parents: []int32{1,4,6},
		Words_mapping:Name,
	}

	b, _ := json.Marshal(temp1)

	fmt.Println("after initialising", string(b))
	fmt.Println("\n Initialising database")
	dir := "../db_data/"

	var temp URL_value
	json.Unmarshal(b, &temp)

	fmt.Println("aaaaaaaaaaaaaaaa", temp.Words_mapping)

	ctx, cancel := context.WithCancel(context.Background())
	log, _ := logger.New("test", 1)

	db, _ := NewBadgerDB(ctx, dir, log)
	defer db.Close(ctx, cancel)
	fmt.Println("before addition")

	db.Iterate(ctx)
	db.Set(ctx, []byte("YES BOI"), b)
	fmt.Println("AFTER ADDITION")
	db.Iterate(ctx)
	c, _ := db.Get(ctx, []byte("YES BOI"))
	var a URL_value
	json.Unmarshal(c, &a)
	fmt.Println("GET FORM DB", a.Words_mapping)
/*
	ctx, cancel := context.WithCancel(context.Background())
	log, _ := logger.New("test", 1)
	fmt.Println("using db_init...")
	inverted, forward, _ := DB_init(ctx, log)
	for _, bdb_i := range inverted {
		defer bdb_i.Close(ctx, cancel)
	}
	for _, bdb := range forward {
		defer bdb.Close(ctx, cancel)
	}

}*/
