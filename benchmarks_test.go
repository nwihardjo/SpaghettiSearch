package main

import (
	"context"
	"crypto/md5"
	"github.com/apsdehal/go-logger"
	"io/ioutil"
	"math/rand"
	"os"
	"the-SearchEngine/database"
	"testing"
)

var ctx context.Context
var frw [5]database.DB

func BenchmarkMD5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = md5.Sum([]byte("https://www.cse.ust.hk/testing/path/urlurl"))
	}
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := frw[2].Get(ctx, []byte("https://www.cse.ust.hk"))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkSet(b *testing.B) {
	word := "new_word"
	hashedW := md5.Sum([]byte(word))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := frw[2].Set(ctx, hashedW[:], []byte(word))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkGetBig(b *testing.B) {
	p := make([]byte, 10000)
	_, _ = rand.Read(p)
	key := []byte("test_get")
	hashedK := md5.Sum(key)
	err := frw[2].Set(ctx, hashedK[:], p[:])
	if err != nil {
		panic(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := frw[2].Get(ctx, hashedK[:])
		if err != nil {
			panic(err)
		}
	}
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	log, _ := logger.New("test", 0, ioutil.Discard)

	inv, forw, _ := database.DB_init(ctx, log)
	for i, v := range forw {
		frw[i] = v
	}

	for _, bdb_i := range inv {
		defer bdb_i.Close(ctx, cancel)
	}
	for _, bdb := range frw {
		defer bdb.Close(ctx, cancel)
	}
	code := m.Run()

	hashedK := md5.Sum([]byte("new_word"))
	e := frw[2].Delete(ctx, hashedK[:])
	if e != nil {
		panic(e)
	}

	key := []byte("test_get")
	hashedK = md5.Sum(key)
	e = frw[2].Delete(ctx, hashedK[:])
	if e != nil {
		panic(e)
	}

	os.Exit(code)
}
