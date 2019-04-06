package database

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"
)

/*
=============================== SCHEMA DEFINITION ==========================================
	Schema for inverted table for both body and title page schema:
		key	: wordId (type: uint32)
		value	: map of DocId to list of positions (type: map[uint16][]uint32)
	Schema for forward table forw[0]:
		key	: word (type: string)
		value	: wordId (type: uint32)
	Schema for forward table forw[1]:
		key	: wordId (type: uint32)
		value	: word (type: string)
	Schema for forward table forw[2]:
		key	: URL (type url.URL)
		value	: DocId (type: uint16)
	Schema for forward table forw[3]:
		key:	: DocId (type: uint16)
		value	: document info including the URL (type: DocInfo)
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

// NOTE: Renamed after URL_value in the previous version
// DocInfo describes the document info and statistics, which serves as the value of forw[2] table (URL -> DocInfo)
type DocInfo struct {
	Url           url.URL           `json:"Url"`
	Page_title    []string          `json:"Page_title"`
	Mod_date      time.Time         `json:"Mod_date"`
	Page_size     uint32            `json:"Page_size"`
	Children      []uint16          `json:"Children"`
	Parents       []uint16          `json:"Parents"`
	Words_mapping map[uint32]uint32 `json:"Words_mapping"`
	//mapping for wordId to wordFrequency
}

// override json.Marshal to support marshalling of DocInfo type
func (u DocInfo) MarshalJSON() ([]byte, error) {
	basicDocInfo := struct {
		Url           string            `json:"Url"`
		Page_title    []string          `json:"Page_title"`
		Mod_date      string            `json:"Mod_date"`
		Page_size     uint32            `json:"Page_size"`
		Children      []uint16          `json:"Children"`
		Parents       []uint16          `json:"Parents"`
		Words_mapping map[uint32]uint32 `json:"Words_mapping"`
	}{u.Url.String(), u.Page_title, u.Mod_date.Format(time.RFC1123), u.Page_size, u.Children, u.Parents, u.Words_mapping}

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

// helper function for type checking and conversion to support schema enforcement 
// uses string approach for primitive data type to be converted to []byte
// uses json marshal function for complex / struct to be converted to []byte
// @return array of bytes, error
func checkMarshal(k interface{}, kType string, v interface{}, vType string)(key []byte, val []byte, err error) { 
	err = nil

	// check the key type
        if kType != "" {
                switch kType {
                case "string":
                        tempKey, ok := k.(string)
                        if !ok { return nil, nil, ErrKeyTypeNotMatch }
			key = []byte(tempKey)
                case "uint16":
                        tempKey, ok := k.(uint16)
                        if !ok { return nil, nil, ErrKeyTypeNotMatch }
			key = []byte(strconv.Itoa(int(tempKey)))
                case "uint32":
                        tempKey, ok := k.(uint32)
                        if !ok { return nil, nil, ErrKeyTypeNotMatch }
			key = []byte(strconv.Itoa(int(tempKey)))
		case "url.URL":
                        tempKey, ok := k.(*url.URL)
                        if !ok { return nil, nil, ErrKeyTypeNotMatch }
			key, err = (*tempKey).MarshalBinary()
		default:
			return nil, nil, ErrKeyTypeNotFound
		}
	} else { 
		key = nil 
	}

	// don't need to check the value type if the key does not matched
	if err != nil { return nil, nil, ErrKeyTypeNotMatch }

        if vType != "" {
                switch vType {
                case "string":
                        tempVal, ok := v.(string)
                        if !ok { return nil, nil, ErrValTypeNotMatch }
			val = []byte(tempVal)
                case "uint16":
                        tempVal, ok := v.(uint16)
                        if !ok { return nil, nil, ErrValTypeNotMatch }
			val = []byte(strconv.Itoa(int(tempVal)))
                case "uint32":
                        tempVal, ok := v.(uint32)
                        if !ok { return nil, nil, ErrValTypeNotMatch }
			val = []byte(strconv.Itoa(int(tempVal)))
                case "map[uint16][]uint32":
                        tempVal, ok := v.(map[uint16][]uint32)
                        if !ok { return nil, nil, ErrValTypeNotMatch }
			val, err = json.Marshal(tempVal)
	        case "DocInfo": 
                        tempVal, ok := v.(DocInfo)
                        if !ok { return nil, nil, ErrValTypeNotMatch }
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
func checkUnmarshal (v []byte, valType string)(val interface{}, err error) {
	switch valType {
	case "string":
		return string(v), nil
	case "uint16":
		tempVal, err := strconv.Atoi(string(v))
		if err != nil { 
			return nil, err
		}
		return uint16(tempVal), nil
	case "uint32":
		tempVal, err := strconv.Atoi(string(v))
		if err != nil { 
			return nil, err
		}
		return uint32(tempVal), nil
	case "map[uint16][]uint32":
		var tempVal = make(map[uint16][]uint32)
		err = json.Unmarshal(v, &tempVal)
		if err != nil { return nil, err }
		return tempVal, nil
	case "DocInfo":
		var tempVal DocInfo
		err = json.Unmarshal(v, &tempVal)
		if err != nil { return nil, err }
		return tempVal, nil
	default:
		return nil, ErrValTypeNotFound
	}
}
