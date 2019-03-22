package main

import (
//	"context"
//	"os"
//	"time"
	"fmt"
	"log"
	"github.com/dgraph-io/badger"
)

const (
	// Default values are used. For garbage-collection purposes
	// TODO: to be fine-tuned
	badgerDiscardRatio = 0.5
	badgerGCInterval = 10 * time.Minute
)


var (
	// BadgerAlertNamespace defines the alerts BadgerDB namespace
	BadgerAlertNamespace = []byte("alerts")
)

type (
	// TODO: investigate namespace necessity
	DB interface {
		Get(namespace, key []byte) (value []byte, err error)
		Set(namespace, key, value []byte) error
		Has(namespace, key []byte) (bool, error)
		Delete(namespace, key []byte) (bool, error)
		Close() error
	}

	BadgerDB struct {
		db	*badger.DB
		logger 	Logger
	}
)

func NewBadgerDB(dir string, logger log.Logger)(DB, error){
	// create directory with specified permission, although badger supports this automatically
	if err := os.MkdirAll(dataDir, 0774); err != nil {
		return nil, err
	}

	opts := badger.DefaultOptions
	// set SyncWrites to False for performance increase but may cause loss of data
	opts.SyncWrites = true
	opts.Dir, opts.ValueDir = dataDir, dataDir
	
	bdb := &BadgerDB {
		db:	badgerDB,
		logger:	logger.With("module", "db"),
	}
	

func main() {
	// testing db
	tmpDir := "./database/"
	
	opts:= badger.DefaultOptions
	opts.Dir = tmpDir
	opts.ValueDir = tmpDir
	db, err := badger.Open(opts)

	if err != nil {
		log.Fatal(err) 
	}
	
	defer db.Close()

	// update db
	err1 := db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte("answer"), []byte("42"))
		return err
	})
	
	if err1 != nil {
		log.Fatal(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("answer"))
	
		if err != nil {log.Fatal(err)}
	
		var valCopy []byte
		err1 := item.Value(func(val []byte) error{
			fmt.Printf("The key 'answer' has value of: %s \n", val)
			valCopy = append([]byte{}, val...)
			return nil
		})

		if err1 != nil {log.Fatal(err)}
		
		fmt.Printf("The key 'answer' has value of: %s \n", valCopy)
		
		return nil
	})

	fmt.Println("db opened and closed")
}
