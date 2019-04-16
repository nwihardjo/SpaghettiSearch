package main

import (
	"time"
	"the-SearchEngine/database"
	"strconv"
	"math/rand"
	"fmt"
	"encoding/hex"
	"context"
	"github.com/apsdehal/go-logger"
	//"net/url"
	//"encoding/json"
	"crypto/md5"
	"sync"
	"io/ioutil"
	bpb "github.com/dgraph-io/badger/pb"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	log, _ := logger.New("test", 0, ioutil.Discard)
	inv, forw, _ := database.DB_init(ctx, log)
	for _, bdb_i := range inv {
		defer bdb_i.Close(ctx, cancel)
	}
	for _, bdb := range forw {
		defer bdb.Close(ctx, cancel)
	}


	n := 100000
/*
	bw := forw[1].BatchWrite_init(ctx) 
	defer bw.Cancel(ctx)
	// populate database
	for i := 0; i < n; i++ {
		p := make([]byte, 16)
		_, _ = rand.Read(p)
		var c []string
		w := make(map[string]uint32)
		for i := 0; i < 200; i++ {
			c = append(c, hex.EncodeToString(p))
			w[hex.EncodeToString(p)] = 100000
		}

		currURL, e := url.Parse("https://www.test.com/"+strconv.Itoa(i*365))
		if e != nil {
			panic(e)
		}
		t := database.DocInfo{
			*currURL,
			nil,
			time.Now(),
			0,
			c,
			nil,
			w,
		}
		key := []byte("https://www.test.com/"+strconv.Itoa(i*365))
		hashedK := md5.Sum(key)
		hashedKStr := hex.EncodeToString(hashedK[:])
		if err := bw.BatchSet(ctx, hashedKStr, t); err != nil {
			panic(err)
		}
	}
	if err := bw.Flush(ctx); err != nil {
		panic(err)
	}

	timer := time.Now()
	data, err := forw[1].Iterate(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("========================= ITERATING FORW[1] TOOK ======================", time.Since(timer))
	
	extractedData := make(map[string][]string, len(data.KV))
	for _, kv := range data.KV{
		var tempVal database.DocInfo
		if err = json.Unmarshal(kv.Value, &tempVal); err != nil {
			panic(err)	
		}

		extractedData[string(kv.Key)] = tempVal.Children
	}
	fmt.Println("it took first %v", time.Since(timer))
*/
	
	bw_ := forw[2].BatchWrite_init(ctx) 
	defer bw_.Cancel(ctx)
	// populate database
	for i := 0; i < n; i++ {
		p := make([]byte, 16)
		_, _ = rand.Read(p)

		var c []string
		for i := 0; i < 200; i++ {
			c = append(c, hex.EncodeToString(p))
		}
		key := []byte("https://www.test.com/"+strconv.Itoa(i*2039))
		hashedK := md5.Sum(key)
		hashedKStr := hex.EncodeToString(hashedK[:])
		if err := bw_.BatchSet(ctx, hashedKStr, c); err != nil {
			panic(err)
		}
	}
	if err := bw_.Flush(ctx); err != nil {
		panic(err)
	}

	timer := time.Now()
	data_, err := forw[2].Iterate(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("======================= ITERATIN FORW[2] TOOK ==================", time.Since(timer))


	timer = time.Now()
	maxNumThreads := 1 
	var wg sync.WaitGroup
	wg.Add(maxNumThreads)
	
	n = len(data_.KV)
	ch := make(chan []string, n)
	
	var dist int
	if n % maxNumThreads > 0 {
		dist = 1 + n / maxNumThreads
	} else {
		dist = n / maxNumThreads
	}

	for i := 0; i < maxNumThreads; i++ {
		start := i * dist
		
		var end int
		if (i + 1) * dist > n {
			end = n
		} else {
			end = (i + 1) * dist
		}

		go parseByteArray(&wg, ch, data_.KV[start:end])
	}
	
	wg.Wait()
	close(ch)	

	dat := make([][]string, n)
	for c := range ch {
		dat = append(dat, c)
	}
	fmt.Println(dat)
	fmt.Println("ITERATING USING 10 THREADS TOOK", time.Since(timer))
/*
	timer = time.Now()		
	extractedData_ := make(map[string][]string, len(data_.KV))
	for _, kv := range data_.KV {
		tempVal := make([]string, len(kv.Value))
		for k, val := range kv.Value {
			tempVal[k] = string(val)
		}
		extractedData_[string(kv.Key)] = tempVal
	}
	fmt.Println("ITERATING THE OLD WAY TOOK", time.Since(timer))
/*

	n = 100000
	temp := make(map[int]int, n)	
 	for i:= 0; i < n; i ++ {
		temp[i] = i
	}
	timer = time.Now()
	for i := 0; i < n; i ++ {
		//fmt.Print(i)	
	}
	fmt.Println("====================")
	fmt.Println(" looping through i ", time.Since(timer))
	
	timer = time.Now()
	for _, _ = range temp {
		//fmt.Print(k)
	}
	fmt.Println("====================")
	fmt.Println(" looping through map ", time.Since(timer)) 
*/
}

func parseByteArray(wg *sync.WaitGroup, ch chan []string, data []*bpb.KV) {
	defer wg.Done()
	
	for _, kv := range data {
		tempVal := make([]string, len(data))
		tempVal[0] = string(kv.Key)
		for k, val := range kv.Value {
			tempVal[k+1] = string(val)
		}
		ch <- tempVal
	}
}	
