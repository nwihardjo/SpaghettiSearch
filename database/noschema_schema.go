package database

import (
	"encoding/json"
	"net/url"
	"strings"
	"time"
	"strconv"
)

/*
=============================== SCHEMA DEFINITION ==========================================
	Schema for inverted table for both body and title page schema:
		key	: wordHash (type: string)
		value	: map of docHash to list of positions (type: map[string][]uint32)
	Schema for forward table forw[0]:
		key	: wordHash (type: string)
		value	: word (type: string)
	Schema for forward table forw[1]:
		key:	: docHash (type: string)
		value	: document info including the URL (type: DocInfo)
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

// NOTE: Renamed after URL_value in the previous version
// DocInfo describes the document info and statistics, which serves as the value of forw[2] table (URL -> DocInfo)
type DocInfo struct {
	Url           url.URL           `json:"Url"`
	Page_title    []string          `json:"Page_title"`
	Mod_date      time.Time         `json:"Mod_date"`
	Page_size     uint32            `json:"Page_size"`
	Children      []string          `json:"Children"`
	Parents       []string          `json:"Parents"`
	Words_mapping map[string]uint32 `json:"Words_mapping"`
	//mapping for wordHash to wordFrequency
}

// override json.Marshal to support marshalling of DocInfo type
func (u DocInfo) MarshalJSON() ([]byte, error) {
	basicDocInfo := struct {
		Url           string            `json:"Url"`
		Page_title    []string          `json:"Page_title"`
		Mod_date      string            `json:"Mod_date"`
		Page_size     uint32            `json:"Page_size"`
		Children      []string          `json:"Children"`
		Parents       []string          `json:"Parents"`
		Words_mapping map[string]uint32 `json:"Words_mapping"`
	}{u.Url.String(), u.Page_title, u.Mod_date.Format(time.RFC1123), u.Page_size,
		u.Children, u.Parents, u.Words_mapping}

	return json.Marshal(basicDocInfo)
}

// override json.Unmarshal to uspport unmarshalling of DocInfo type
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
			u.Children = make([]string, len(v.([]interface{})))
			for k_, v_ := range v.([]interface{}) {
				u.Children[k_] = v_.(string)
			}
		case "parents":
			u.Parents = make([]string, len(v.([]interface{})))
			for k_, v_ := range v.([]interface{}) {
				u.Parents[k_] = v_.(string)
			}
		case "words_mapping":
			u.Words_mapping = make(map[string]uint32)
			for k_, v_ := range v.(map[string]interface{}) {
				u.Words_mapping[k_] = uint32(v_.(float64))
			}
		}
	}

	return nil
}

// helper function for type checking and conversion to support schema enforcement
// uses string approach for primitive data type to be converted to []byte
// uses json marshal function for complex / struct to be converted to []byte
// @return array of bytes, error
func checkMarshal(k interface{}, kType string, v interface{}, vType string) (key []byte, val []byte, err error) {
	err = nil

	// check the key type
	if kType != "" {
		switch kType {
		case "string":
			tempKey, ok := k.(string)
			if !ok {
				return nil, nil, ErrKeyTypeNotMatch
			}
			key = []byte(tempKey)
		default:
			return nil, nil, ErrKeyTypeNotFound
		}
	} else {
		key = nil
	}

	// don't need to check the value type if the key does not matched
	if err != nil {
		return nil, nil, ErrKeyTypeNotMatch
	}

	if vType != "" {
		switch vType {
		case "string":
			tempVal, ok := v.(string)
			if !ok {
				return nil, nil, ErrValTypeNotMatch
			}
			val = []byte(tempVal)
		case "[]string":
			tempVal, ok := v.([]string)
			if !ok {
				return nil, nil, ErrValTypeNotMatch
			}
			val, err = json.Marshal(tempVal)
		case "float64":
			tempVal, ok := v.(float64)
			if !ok {
				return nil, nil, ErrValTypeNotMatch
			}
			val, err = []byte(strconv.FormatFloat(tempVal, 'f', -1, 64)), nil
		case "map[string][]uint32":
			tempVal, ok := v.(map[string][]uint32)
			if !ok {
				return nil, nil, ErrValTypeNotMatch
			}
			val, err = json.Marshal(tempVal)
		case "DocInfo":
			tempVal, ok := v.(DocInfo)
			if !ok {
				return nil, nil, ErrValTypeNotMatch
			}
			val, err = json.Marshal(tempVal)
		default:
			return nil, nil, ErrValTypeNotFound
		}
	} else {
		val = nil
	}

	return
}

// helper function for type checking and conversion to support schema enforcement
func checkUnmarshal(v []byte, valType string) (val interface{}, err error) {
	switch valType {
	case "string":
		return string(v), nil
	case "[]string":
		var tempVal []string
		if err = json.Unmarshal(v, &tempVal); err != nil {
			return nil, err
		}
		return tempVal, nil
	case "float64":
		return strconv.ParseFloat(string(v), 64)
	case "map[string][]uint32":
		tempVal := make(map[string][]uint32)
		err = json.Unmarshal(v, &tempVal)
		if err != nil {
			return nil, err
		}
		return tempVal, nil
	case "DocInfo":
		var tempVal DocInfo
		err = json.Unmarshal(v, &tempVal)
		if err != nil {
			return nil, err
		}
		return tempVal, nil
	default:
		return nil, ErrValTypeNotFound
	}
}
