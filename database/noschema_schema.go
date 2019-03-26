//package noschema_schema
package main


import (
	"fmt"
	"encoding/json"
	//"os"
)

type InvKeyword_value struct {
	DocId	int32
	Pos	[]int32
}

type InvKeyword_values []InvKeyword_value

//TODO: to be completed date type
type URL_value struct {
	DocId 		int32
	Mod_date	string
	Page_size	int32
	Childrens	[]int32
	Parents		[]int32
	Words_mapping	map[int32]int32 //mapping for wordId to wordFrequency
}

//TODO: perform benchmarking between using struct and space in between URL and location
type DocId_value struct {
	URL	string
	FileLoc	string
}

func main() {
	
}
