package main

import (
	"context"
	"fmt"
	"github.com/apsdehal/go-logger"
	"the-SearchEngine/database"
)

func main() {
	ctx, cancel := context.WithCancel(context.TODO())
	log, _ := logger.New("test", 1)
	inv, forw, _ := database.DB_init(ctx, log)
	for i, bdb_i := range inv {
		fmt.Println("============================== Inverted", i, "====================================")
		bdb_i.Debug_Print(ctx)
		fmt.Println("==================================================================================")
		fmt.Println("\n")
		defer bdb_i.Close(ctx, cancel)
	}
	for i, bdb := range forw {
		fmt.Println("============================== Forward ", i, "====================================")
		bdb.Debug_Print(ctx)
		fmt.Println("==================================================================================")
		fmt.Println("\n")
		defer bdb.Close(ctx, cancel)
	}
}
