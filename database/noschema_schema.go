package database

//package main

import (
	//"context"
	"encoding/json"
	//"fmt"
	//"github.com/apsdehal/go-logger"
	//"net/url"
	"strconv"
	"strings"
	"time"
)

/*
=============================== SCHEMA DEFINITION ==========================================

	Schema for inverted table for both body and title page schema:
		key	: DocId (type: int32)
		value	: list of InvKeyword_value, where each contain the DocId and positions fo the word (type: InvKeyword_values, see InvKeyword_value)

	Schema for forward table forw[0]:
		key	: word (type: string)
		value	: wordId (type: int32)

	Schema for forward table forw[1]:
		key	: wordId (type: int32)
		value	: word (type: string)

	Schema for forward table forw[2]:
		key	: URL (type url.URL)
		value	: document info including DocId (type: DocInfo)

	Schema for forward table forw[3]:
		key:	: DocId (type: int32)
		value	: URL (type: url.URL)

	Schema for forward table forw[4]:
		key	: index type (type: string)
		value	: biggest index value (type: int32)

========================= MARSHAL AND UNMARSHALING =======================================

	Unless specified, all data structure (particularly the primitive ones) can be casted into array of bytes as below. Then the data can be passed for Set or any operation on the table object.

		byteArray, err := json.Marshal(any_data_type_unless_specified)


	To cast back into the desired data type, use Unmarshal operation

		byteArray, err := tableObject.Get(some_context, key_in_byteArray)
		var a desired_datatype
		err = json.Unmarshal(byteArray, &a)


	For url.URL data type, use command below to both encode it into array of byte and vice versa

		urlObject, err := url.Parse(url_in_string)
		byteArray, err := urlObject.MarshalBinary()

		tempUrl := &url.URL
		err := tempUrl.UnmarshalBinary(byteArray)
*/

// Each item in the a value of inverted table contains the DocId (type: int32) and list of position of the word location in the document
type InvKeyword_value struct {
	DocId int32   `json:"DocId"`
	Pos   []int32 `json:"Pos"` // list of position of the word occuring in the document DocId
}

// InvKeyword_values contains slice of InvKeyword_value to support append operation
type InvKeyword_values []InvKeyword_value

// NOTE: Renamed after URL_value in the previous version
// DocInfo describes the document info and statistics, which serves as the value of forw[2] table (URL -> DocInfo)
type DocInfo struct {
	DocId         int32           `json:"DocId"`
	Mod_date      time.Time       `json:"Mod_date"`
	Page_size     int32           `json:"Page_size"`
	Children      []int32         `json:"Childrens"`
	Parents       []int32         `json:"Parents"`
	Words_mapping map[int32]int32 `json:"Words_mapping"`
	//mapping for wordId to wordFrequency
}

func (u DocInfo) MarshalJSON() ([]byte, error) {
	basicDocInfo := struct {
		DocId         int32           `json:"DocId"`
		Mod_date      string          `json:"Mod_date"`
		Page_size     int32           `json:"Page_size"`
		Children      []int32         `json:"Childrens"`
		Parents       []int32         `json:"Parents"`
		Words_mapping map[int32]int32 `json:"Words_mapping"`
	}{u.DocId, u.Mod_date.Format(time.RFC1123), u.Page_size, u.Children, u.Parents, u.Words_mapping}

	return json.Marshal(basicDocInfo)
}

func (u *DocInfo) UnmarshalJSON(j []byte) error {
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

/*func main() {
	temp1, _ := url.Parse("https://www.google.com")
	b, _ := temp1.MarshalBinary()

	fmt.Println("after initialising", string(b))

	temp := &url.URL{}
	_ = temp.UnmarshalBinary(b)

	fmt.Println("after unmarshaling", temp)

	dir := "../db_data/"

	Name := make(map[int32]int32)
	Name[0] = 0
	Name[1] = 12
	Name[2] = 23
	Name[3] = 40
	tempdocinfo := DocInfo{
		DocId: 1,
		Mod_date: time.Now(),
		Page_size: 1,
		Children: []int32{1,2,3},
		Parents: []int32{1,4,6},
		Words_mapping:Name,
	}

	b1, _ := json.Marshal(tempdocinfo)
	fmt.Println("after initialising", string(b1))
	var tempb1 DocInfo

	json.Unmarshal(b1, &tempb1)
	fmt.Println("after unmarshaling", tempb1.Words_mapping)

	ctx, cancel := context.WithCancel(context.Background())
	log, _ := logger.New("test", 1)

	db, _ := NewBadgerDB(ctx, dir, log)
	defer db.Close(ctx, cancel)
	fmt.Println("BEFORE ADDITION")

	db.Iterate(ctx)
	db.Set(ctx, []byte("1"), b)
	db.Set(ctx, []byte("2"), b1)
	fmt.Println("AFTER ADDITION")
	db.Iterate(ctx)
	c, _ := db.Get(ctx, []byte("1"))
	d, _ := db.Get(ctx, []byte("2"))
	temp2 := &url.URL{}
	temp2.UnmarshalBinary(c)
	var tempd DocInfo
	json.Unmarshal(d, &tempd)
	fmt.Println("GET FROM DB", temp2)
	fmt.Println("GET FROM DB", tempd.Words_mapping)
}

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
