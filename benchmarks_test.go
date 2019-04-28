package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"github.com/apsdehal/go-logger"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"testing"
	"the-SearchEngine/database"
	"time"
	"fmt"
	"github.com/deckarep/golang-set"
	"github.com/juliangruber/go-intersect"
	"github.com/thoas/go-funk"
)

var ctx context.Context
var frw [5]database.DB

func BenchmarkMD5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = md5.Sum([]byte("https://www.cse.ust.hk/testing/path/urlurl"))
	}
}

func BenchmarkGetWord(b *testing.B) {
	word := "new_word"
	hashedW := md5.Sum([]byte(word))
	hashedWStr := hex.EncodeToString(hashedW[:])
	err := frw[0].Set(ctx, hashedWStr, word)
	if err != nil {
		panic(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = frw[0].Get(ctx, hashedWStr)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkGet200Children200Words(b *testing.B) {
	p := make([]byte, 16)
	_, _ = rand.Read(p)
	var c []string
	w := make(map[string]uint32)
	for i := 0; i < 200; i++ {
		c = append(c, hex.EncodeToString(p))
		w[hex.EncodeToString(p)] = 100000
	}

	currURL, e := url.Parse("https://www.test.com")
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

	key := []byte("https://www.test.com")
	hashedK := md5.Sum(key)
	hashedKStr := hex.EncodeToString(hashedK[:])
	if err := frw[1].Set(ctx, hashedKStr, t); err != nil {
		panic(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := frw[1].Get(ctx, hashedKStr)
		if err != nil {
			panic(err)
		}
	}
}

	

func BenchmarkSetWord(b *testing.B) {
	word := "new_word"
	hashedW := md5.Sum([]byte(word))
	hashedWStr := hex.EncodeToString(hashedW[:])
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := frw[0].Set(ctx, hashedWStr, word)
		if err != nil {
			panic(err)
		}
	}
}

var slice1 []float64
var slice2 []float64
var slice3 []float64

func Benchmark_gointersect(b *testing.B) {
	temp := intersect.Simple(slice1, slice2)
	temp = intersect.Simple(temp, slice3)
	fmt.Println(temp)
}

func Benchmark_golangset(b *testing.B) {
	s1 := mapset.NewSet()
	s2 := mapset.NewSet()
	s3 := mapset.NewSet()
	for _, i := range slice1 {
		s1.Add(i)
	}
	for _, i := range slice2 {
		s2.Add(i)
	} 
	for _, i := range slice3 {
		s3.Add(i)
	}
	
	b.ResetTimer()
	temp := s1.Intersect(s2)
	temp = temp.Intersect(s3)
	fmt.Println(temp)
}

func Benchmark_gofunc(b *testing.B) {
	temp := funk.Intersect(slice1, slice2)
	temp = funk.Intersect(temp, slice3)
	fmt.Println(temp)
}

	
func randFloats(min, max int, n int) []float32 {
	res := make([]float32, n)
	for i := range res {
		res[i] = float32(min + rand.Int()*(max-min))
	}
	return res
}


func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())
	slice1 = randFloats(1, 1000, 5000)
	slice2 = randFloats(1, 1000, 5000)
	slice3 = randFloats(1, 1000, 5000)

	ctx, cancel := context.WithCancel(context.Background())
	log, _ := logger.New("test", 0, ioutil.Discard)

	inv, forw, _ := database.DB_init(ctx, log)
	for i, v := range forw {
		frw[i] = v
	}

	for _, bdb_i := range inv {
		defer bdb_i.Close(ctx, cancel)
	}
	for _, bdb := range forw {
		defer bdb.Close(ctx, cancel)
	}
	code := m.Run()

	hashedK := md5.Sum([]byte("new_word"))
	hashedKStr := hex.EncodeToString(hashedK[:])
	e := frw[0].Delete(ctx, hashedKStr)
	if e != nil {
		panic(e)
	}

	key := []byte("https://www.test.com")
	hashedK = md5.Sum(key)
	hashedKStr = hex.EncodeToString(hashedK[:])
	e = frw[1].Delete(ctx, hashedKStr)
	if e != nil {
		panic(e)
	}

	os.Exit(code)
}
