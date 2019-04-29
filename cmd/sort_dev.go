package main

import (
	"time"
	"fmt"
	"sort"
)

type kv struct {
	Key string
	Value uint32
}

func main() {
	m := map[string]uint32 {"a":10, "b":20, "c":30,}
	timer := time.Now()
	ss := make([]kv, len(m))
	for k, v := range m {
		ss = append(ss, kv{k, v})
	}
	
	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})
	count := 0
	for _, kv := range ss{
		fmt.Printf("%s, %d\n", kv.Key, kv.Value)
		count ++ 
		if count == len(m) {
			break
		}
	}
	fmt.Println(time.Since(timer))
	
	k
}
