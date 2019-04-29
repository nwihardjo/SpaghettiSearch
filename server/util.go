package main

import (
	"time"
	"regexp"
	"sort"
	db "the-SearchEngine/database"
)

type Rank_term struct {
	TitleWeights	[]float32
	BodyWeights	[]float32
	// used only for phrase search
	TermPos		uint8
}

type Rank_result struct {
	DocHash		string
	TitleRank	float64
	BodyRank	float64
}

type Rank_combined struct {
	Url           string        		   `json:"Url"`
	Page_title    []string       		   `json:"Page_title"`
	Mod_date      time.Time      		   `json:"Mod_date"`
	Page_size     uint32         		   `json:"Page_size"`
	Children      []string       		   `json:"Children"`
	Parents       []string	   		   `json:"Parents"`
	Words_mapping map[string]uint32 	   `json:"Words_mapping"`
	PageRank      float64			   `json:"PageRank"`
	FinalRank     float64			   `json:"FinalRank"`
}

type termPhrase struct {
	Term	string
	Pos	uint8
}

func appendSort(data []Rank_combined, el Rank_combined) []Rank_combined {
	index := sort.Search(len(data), func(i int) bool { return data[i].FinalRank < el.FinalRank })
	data = append(data, Rank_combined{})
	copy(data[index+1:], data[index:])
	data[index] = el
	return data
}

func resultFormat(metadata db.DocInfo, PR float64, finalRank float64) Rank_combined {
	parentList := make([]string, len(metadata.Parents))
	idx := 0
	for parentHash, _ := range metadata.Parents {
		parentList[idx] = parentHash
		idx++
	}

	return Rank_combined {
		Url		:	metadata.Url.String(),
		Page_title	:	metadata.Page_title,
		Mod_date	:	metadata.Mod_date,
		Page_size	:	metadata.Page_size,
		Children	:	metadata.Children,
		Parents		:	parentList,
		Words_mapping	:	metadata.Words_mapping,
		PageRank	:	PR,
		FinalRank	:	finalRank,
	}
}

var re = regexp.MustCompile(`".*?"`)
func getPhrase(s string) []string {
	ms := re.FindAllString(s, -1)
	ss := make([]string, len(ms))
	for i, m := range ms {
		ss[i] = m[1 : len(m)-1]
	}
	return ss
}

func sortFloat32(slice []float32) []float32 {
	sliceFloat64 := make([]float64, len(slice))
	for i := 0; i < len(slice); i++ {
		sliceFloat64[i] = float64(slice[i])
	}

	sort.Float64s(sliceFloat64)
	for i := 0; i < len(slice); i++ {
		slice[i] = float32(sliceFloat64[i])
	}
	return slice
}

func intersect(slice1, slice2 []float32) []float32 {
	var ret []float32

	// sort slice first based on sort library
	slice1 = sortFloat32(slice1)
	slice2 = sortFloat32(slice2)
	
	i, j := 0, 0
	for i != len(slice1) && j != len(slice2) {
		if slice1[i] == slice2[j] {
			ret = append(ret, slice1[i])
			i += 1
			j += 1
		} else if slice1[i] > slice2[j] {
			j += 1
		} else {
			i += 1
		}
	}
	return ret
}
