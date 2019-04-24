package main

import (
	"fmt"
	"context"
	"encoding/json"
	db "the-SearchEngine/database"
	"github.com/apsdehal/go-logger"
	"runtime"
)

type tableCollector struct {
	Key	string
	Val	map[string][]uint32 
}

func main() {
	PrintMemUsage()
	ctx, cancel := context.WithCancel(context.TODO())
	log, _ := logger.New("test", 1)
	inv, forw, _ := db.DB_init(ctx, log)
	for _, bdb_i := range inv {
		defer bdb_i.Close(ctx, cancel)
	}
	for _, bdb := range forw{
		defer bdb.Close(ctx, cancel)
	}

	PrintMemUsage()
	comp, _ := inv[0].Iterate(ctx)
	KV := make([]tableCollector, len(comp.KV))
	for i := 0; i < len(comp.KV); i++ {
		KV[i].Key = string(comp.KV[i].Key)
		json.Unmarshal(comp.KV[i].Value, &(KV[i].Val))
	}
	PrintMemUsage()

	comp, _ = inv[1].Iterate(ctx)
	KV = make([]tableCollector, len(comp.KV))
	for i := 0; i < len(comp.KV); i++ {
		KV[i].Key = string(comp.KV[i].Key)
		json.Unmarshal(comp.KV[i].Value, &(KV[i].Val))
	}
	PrintMemUsage()
	runtime.GC()
	runtime.GC()
	runtime.GC()
	PrintMemUsage()
}
func PrintMemUsage() {
        var m runtime.MemStats
        runtime.ReadMemStats(&m)
        // For info on each, see: https://golang.org/pkg/runtime/#MemStats
        fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
        fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
        fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
        fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
    return b / 1024 / 1024
}


