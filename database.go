package db_wrapper

import (
	"context"
	"os"
	"time"
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
	// TODO: investigate namespace necessity for additional arguments
	// TODO: add logger debug in each function
	DB interface {
		Get(ctx context.Context, key []byte) (value []byte, err error)
		Set(ctx context.Context, key [] byte, value []byte) error
		Has(ctx context.Context, key []byte) (bool, error)
		Delete(ctx context.Context, key []byte) error
		Close(ctx context.Context) error
	}

	BadgerDB struct {
		db	*badger.DB
		logger 	Logger
	}
)

func NewBadgerDB (ctx context.Context, dir string, logger log.Logger)(DB, error){
	// will open an existing directory, or create a new one if dir not exist 

	opts := badger.DefaultOptions
	// set SyncWrites to False for performance increase but may cause loss of data
	opts.SyncWrites = true
	opts.Dir, opts.ValueDir = dataDir, dataDir

	badgerDB, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}	
		
	bdb := &BadgerDB {
		db:	badgerDB,
		// TODO: double check, possible bug
		logger:	logger.With("module", "db"),
	}

	// run garbage collection in advance	
	go bdb.runGC(ctx)
	return bdb, nil
}

func (bdb *BadgerDB) Get(ctx context.Context, key []byte) (value []byte, err error) {
	err = bdb.db.View(func(txn *badger.Txn) error{
		item, err := txn.Get(key)
		
		if err != nil {
			log.Fatal(err)
			return err 
		}
	
		// value needed to be copied as it only lasts when the transaction is open
		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})

		if err != nil {
			log.Fatal(err)
			return err
		}
		return nil
	})

	if err != nil { return nil, err }
	return value, nil

func (bdb *BadgerDB) Set(ctx context.Context, key []byte, value []byte) error {
	err := bdb.db.Update(func txn *badger.Txn) error {
		return txn.Set(key, value)
	})

	if err != nil {
		bdb.logger.Debugf("Failed to set key %s: %v", key, value)
		return err
	}
	return nil
}

func (bdb *BadgerDB) Has(ctx context.Context, key []byte) (ok bool, err error) {
	_, err = bdb.Get(ctx, key)
	switch err {
		case badger.ErrKeyNotFound:
			ok, err = false, nil
		case nil:
			ok, err = true, nil
	}
	return
}

func (bdb *BadgerDB) Delete(ctx context.Context, key []byte) error {
	err := bdb.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err != nil {
			bdb.logger.Debugf("Failed to delete key: %v")
			return err 
		}
		return nil
	})
	return err
}

func (bdb *BadgerDB) Close() error {
	return bdb.db.Close()
}

func (bdb *BadgerDB) runGC(ctx context.Context){
	ticker := time.NewTicker(badgerGCInterval)
	for {
		select {
		case <- ticker.C:
			err := bdb.db.RunValueLogGC(badgerDiscardRatio)
			if err != nil {
				if err == badger.ErrNoRewrite {
					bdb.logger.Debugf("No BadgerDB GC occured: %v", err)
				} else {
					bdb.logger.Errorf("Failed to GC BadgerDB: %v", err)
				}
			}
		case <- ctx.Done():
			return
		}
	}
}

//func main() {
	// testing db
//	tmpDir := "./database/"
//	opts:= badger.DefaultOptions
//	opts.Dir = tmpDir
//	opts.ValueDir = tmpDir
//	db, err := badger.Open(opts)
//
//	if err != nil {
//		log.Fatal(err) 
//	}
//	
//	defer db.Close()
//
//	// update db
//	err1 := db.Update(func(txn *badger.Txn) error {
//		err := txn.Set([]byte("answer"), []byte("42"))
//		return err
//	})
//	
//	if err1 != nil {
//		log.Fatal(err)
//	}
//
//	err = db.View(func(txn *badger.Txn) error {
//		item, err := txn.Get([]byte("answer"))
//	
//		if err != nil {log.Fatal(err)}
//	
//		var valCopy []byte
//		err1 := item.Value(func(val []byte) error{
//			fmt.Printf("The key 'answer' has value of: %s \n", val)
//			valCopy = append([]byte{}, val...)
//			return nil
//		})
//
//		if err1 != nil {log.Fatal(err)}
//		
//		fmt.Printf("The key 'answer' has value of: %s \n", valCopy)
//		
//		return nil
//	})
//
//	fmt.Println("db opened and closed")
//}
