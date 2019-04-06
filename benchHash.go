package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/apsdehal/go-logger"
	"the-SearchEngine/database"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	log, _ := logger.New("test", 1)

	inv, frw, _ := database.DB_init(ctx, log)

	for _, bdb_i := range inv {
		defer bdb_i.Close(ctx, cancel)
	}
	for _, bdb := range frw {
		defer bdb.Close(ctx, cancel)
	}

	t1 := time.Now()
	_, err := frw[2].Get(ctx, "https://www.cse.ust.hk")
	if err != nil {
		panic(err)
	}
	fmt.Println("DB Get overhead:", time.Now().Sub(t1).String())

	t1 = time.Now()
	_ = md5.Sum([]byte("https://www.cse.ust.hk"))
	fmt.Println("MD5 Hash overhead:", time.Now().Sub(t1).String())
}
