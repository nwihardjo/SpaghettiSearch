package main

import (
	"./database"
	"context"
	"github.com/apsdehal/go-logger"
	"fmt"
)

func main() {
	ctx, cancel := context.WithCancel(context.TODO())
	logg, _ := logger.New("test", 1)
	inv , forw, _ := database.DB_init(ctx, logg)
	for _, v := range inv{ defer v.Close(ctx, cancel) }
	for _, v := range forw{ defer v.Close(ctx, cancel) }
	
	forw[0].Debug_Print(ctx)
	val, err := forw[0].Get(ctx, "video")
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	fmt.Println(val)
}
