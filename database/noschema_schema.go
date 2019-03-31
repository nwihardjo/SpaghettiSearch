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

/*
=============================== SCHEMA DEFINITION ==========================================
	Schema for inverted table for both body and title page schema:
		key	: wordId (type: uint32)
		value	: list of InvKeyword_value, where each contain the DocId and positions fo the word (type: InvKeyword_values, see InvKeyword_value)
	Schema for forward table forw[0]:
		key	: word (type: string)
		value	: wordId (type: uint32)
	Schema for forward table forw[1]:
		key	: wordId (type: uint32)
		value	: word (type: string)
	Schema for forward table forw[2]:
		key	: URL (type url.URL)
		value	: document info including DocId (type: DocInfo)
	Schema for forward table forw[3]:
		key:	: DocId (type: uint16)
		value	: URL (type: url.URL)
	Schema for forward table forw[4]:
		key	: index type (type: string)
		value	: biggest index value (type: uint32)
========================= MARSHAL AND UNMARSHALING =======================================
	Unless specified, all defined struct can be casted into array of bytes as below. Then the data can be passed for Set or any operation on the table object.
		byteArray, err := json.Marshal(any_struct_defined_in_this_file)
	To cast back into the desired data type, use Unmarshal operation
		byteArray, err := tableObject.Get(some_context, key_in_byteArray)
		var a desired_datatype
		err = json.Unmarshal(byteArray, &a)
	For url.URL data type, use command below to both encode it into array of byte and vice versa
		urlObject, err := url.Parse(url_in_string)
		byteArray, err := urlObject.MarshalBinary()
		tempUrl := &url.URL{}
		err := tempUrl.UnmarshalBinary(byteArray)
*/

// Each item in the a value of inverted table contains the DocId (type: uint16) and list of position of the word location in the document
type InvKeyword_value struct {
	DocId uint16   `json:"DocId"`
	Pos   []uint32 `json:"Pos"` // list of position of the word occuring in the document DocId
}

// InvKeyword_values contains slice of InvKeyword_value to support append operation
type InvKeyword_values []InvKeyword_value

// NOTE: Renamed after URL_value in the previous version
// DocInfo describes the document info and statistics, which serves as the value of forw[2] table (URL -> DocInfo)
type DocInfo struct {
	Url         url.URL            `json:"Url"`
	Page_title    []string          `json:"Page_title"`
	Mod_date      time.Time         `json:"Mod_date"`
	Page_size     uint32            `json:"Page_size"`
	Children      []uint16          `json:"Children"`
	Parents       []uint16          `json:"Parents"`
	Words_mapping map[uint32]uint32 `json:"Words_mapping"`
	//mapping for wordId to wordFrequency
}

func (u DocInfo) MarshalJSON() ([]byte, error) {
	basicDocInfo := struct {
		Url         string            `json:"Url"`
		Page_title    []string          `json:"Page_title"`
		Mod_date      string            `json:"Mod_date"`
		Page_size     uint32            `json:"Page_size"`
		Children      []uint16          `json:"Children"`
		Parents       []uint16          `json:"Parents"`
		Words_mapping map[uint32]uint32 `json:"Words_mapping"`
	}{u.Url.String(), u.Page_title, u.Mod_date.Format(time.RFC1123), u.Page_size, u.Children, u.Parents, u.Words_mapping}

	return json.Marshal(basicDocInfo)
}

func (u *DocInfo) UnmarshalJSON(j []byte) error {
	var rawStrings map[string]interface{}

	err := json.Unmarshal(j, &rawStrings)
	if err != nil {
		return err
	}

	for k, v := range rawStrings {
		if v == nil {
			continue
		}
		switch strings.ToLower(k) {
		case "url":
			temp, err := url.Parse(v.(string))
			if err != nil {
				return err
			}
			u.Url = *temp
		case "page_title":
			u.Page_title = make([]string, len(v.([]interface{})))
			for k_, v_ := range v.([]interface{}) {
				u.Page_title[k_] = v_.(string)
			}
		case "mod_date":
			if u.Mod_date, err = time.Parse(time.RFC1123, v.(string)); err != nil {
				return err
			}
		case "page_size":
			u.Page_size = uint32(v.(float64))
		case "children":
			u.Children = make([]uint16, len(v.([]interface{})))
			for k_, v_ := range v.([]interface{}) {
				u.Children[k_] = uint16(v_.(float64))
			}
		case "parents":
			u.Parents = make([]uint16, len(v.([]interface{})))
			for k_, v_ := range v.([]interface{}) {
				u.Parents[k_] = uint16(v_.(float64))
			}
		case "words_mapping":
			u.Words_mapping = make(map[uint32]uint32)
			for k_, v_ := range v.(map[string]interface{}) {
				str, _ := strconv.ParseInt(k_, 0, 32)
				u.Words_mapping[uint32(str)] = uint32(v_.(float64))
			}
		}
	}

	return nil
}
