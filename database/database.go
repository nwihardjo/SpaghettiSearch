package database

//package main

import (
	"context"
	"encoding/json"
	"github.com/apsdehal/go-logger"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	bpb "github.com/dgraph-io/badger/pb"
	"log"
	"net/url"
	"os"
	"time"
	//"fmt"
)

const (
	// Default values are used. For garbage-collection purposes
	// TODO: to be fine-tuned
	badgerDiscardRatio = 0.5
	badgerGCInterval   = 10 * time.Minute
)

var (
	// BadgerAlertNamespace defines the alerts BadgerDB namespace
	BadgerAlertNamespace = []byte("alerts")
)

type (
	// Set method should be passed InvKeyword_values as the underlying datatype of the value
	// AppendValue method should be passed InvKeyword_value as the underlying datatype of the appended value

	DB_Inverted interface {
		DB
		AppendValue(ctx context.Context, key []byte, appendedValue []byte) error
	}

	BadgerDB_Inverted struct {
		BadgerDB
	}
)

type (
	// TODO: add logger debug in each function
	DB interface {
		// TODO: integrate prefix search
		Get(ctx context.Context, key []byte) (value []byte, err error)
		Set(ctx context.Context, key []byte, value []byte) error
		Has(ctx context.Context, key []byte) (bool, error)
		Delete(ctx context.Context, key []byte) error
		Close(ctx context.Context, cancel context.CancelFunc) error
		// DropTable will remove all the data in the table
		DropTable(ctx context.Context) error
		// data is in random , due to concurrency
		// ONLY PERFORM THIS ON forw[2] DUE TO DATATYPE. Other datatype will be supported in future release
		Iterate(ctx context.Context) (map[url.URL]DocInfo, error)
	}

	BadgerDB struct {
		db     *badger.DB
		logger *logger.Logger
	}
)

/*
	object passed on DB_init should be used as global variable, only call DB_init once (operation on database object can be concurrent)

	refer to `noschema_schema.go` for each table's key and value data types

	\params: context, logger
	\return: list of inverted tables (type: []DB_Inverted), list of forward tables (type: []DB), error
		inv[0]: inverted table for keywords in body section
		inv[1]: inverted table for keywords in title section
		forw[0]: forward table for word to wordId mapping
		forw[1]: forward table for wordId to word mapping
		forw[2]: forward table for URL to docInfo (including DocId) mapping
		forw[3]: forward table for DocId to URL mapping
		forw[4]: forward table for keeping track of index
*/

func DB_init(ctx context.Context, logger *logger.Logger) (inv []DB_Inverted, forw []DB, err error) {
	base_dir := "./db_data/"
	inverted_dir := map[string]bool{"invKeyword_body/": false, "invKeyword_title/": false}
	forward_dir := map[string]bool{"Word_wordId/": false, "WordId_word": false, "URL_docId/": false, "DocId_URL/": false, "Indexes/": true}

	// create directory if not exist
	for d, _ := range inverted_dir {
		if _, err := os.Stat(base_dir + d); os.IsNotExist(err) {
			os.Mkdir(base_dir+d, 0755)
		}
	}

	for d, _ := range forward_dir {
		if _, err := os.Stat(base_dir + d); os.IsNotExist(err) {
			os.Mkdir(base_dir+d, 0755)
		}
	}

	// initiate table object
	for k, v := range inverted_dir {
		temp, err := NewBadgerDB_Inverted(ctx, base_dir+k, logger, v)
		if err != nil {
			log.Fatal(err)
			return nil, nil, err
		}
		inv = append(inv, temp)
	}

	for k, v := range forward_dir {
		temp, err := NewBadgerDB(ctx, base_dir+k, logger, v)
		if err != nil {
			log.Fatal(err)
			return nil, nil, err
		}
		forw = append(forw, temp)
	}

	return inv, forw, nil
}

func NewBadgerDB_Inverted(ctx context.Context, dir string, logger *logger.Logger, loadIntoRAM bool) (DB_Inverted, error) {
	opts := badger.DefaultOptions
	if loadIntoRAM {
		// How should LSM tree be accessed
		opts.TableLoadingMode = options.LoadToRAM
	}
	// set SyncWrites to False for performance increase but may cause loss of data
	opts.SyncWrites = true
	opts.Dir, opts.ValueDir = dir, dir

	badgerDB, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	bdb_i := &BadgerDB_Inverted{BadgerDB{badgerDB, logger}}

	// run garbage collection in advance
	go bdb_i.runGC(ctx)
	return bdb_i, nil
}

func NewBadgerDB(ctx context.Context, dir string, logger *logger.Logger, loadIntoRAM bool) (DB, error) {
	opts := badger.DefaultOptions
	if loadIntoRAM {
		// How should LSM tree be accessed
		opts.TableLoadingMode = options.LoadToRAM
	}
	// set SyncWrites to False for performance increase but may cause loss of data
	opts.SyncWrites = true
	opts.Dir, opts.ValueDir = dir, dir

	badgerDB, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	bdb := &BadgerDB{
		db:     badgerDB,
		logger: logger,
	}

	// run garbage collection in advance
	go bdb.runGC(ctx)
	return bdb, nil
}

func (bdb *BadgerDB) DropTable(ctx context.Context) error {
	return bdb.db.DropAll()
}

func (bdb_i *BadgerDB_Inverted) AppendValue(ctx context.Context, key []byte, appendedValue []byte) error {
	value, err := bdb_i.Get(ctx, key)
	if err != nil {
		log.Fatal(err)
		return err
	}

	var appendedValue_struct InvKeyword_value
	var tempValues InvKeyword_values
	err = json.Unmarshal(value, &tempValues)
	if err != nil {
		log.Fatal(err)
		return err
	}
	err = json.Unmarshal(appendedValue, &appendedValue_struct)
	if err != nil {
		log.Fatal(err)
		return err
	}

	tempValues = append(tempValues, appendedValue_struct)
	tempVal, err := json.Marshal(tempValues)
	if err != nil {
		log.Fatal(err)
		return err
	}

	// delete and set the new appended values
	// TODO: optimise the operation
	if err = bdb_i.Delete(ctx, key); err != nil {
		log.Fatal(err)
		return err
	}
	if err = bdb_i.Set(ctx, key, tempVal); err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

func (bdb *BadgerDB) Get(ctx context.Context, key []byte) (value []byte, err error) {
	err = bdb.db.View(func(txn *badger.Txn) error {
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

	if err != nil {
		return nil, err
	}
	return value, nil
}

func (bdb *BadgerDB) Set(ctx context.Context, key []byte, value []byte) error {
	err := bdb.db.Update(func(txn *badger.Txn) error {
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

func (bdb *BadgerDB) Close(ctx context.Context, cancel context.CancelFunc) error {
	// perform cancellation of the running process using context
	cancel()
	return bdb.db.Close()
}

func (bdb *BadgerDB) runGC(ctx context.Context) {
	ticker := time.NewTicker(badgerGCInterval)
	for {
		select {
		case <-ticker.C:
			err := bdb.db.RunValueLogGC(badgerDiscardRatio)
			if err != nil {
				if err == badger.ErrNoRewrite {
					bdb.logger.Debugf("No BadgerDB GC occured: %v", err)
				} else {
					bdb.logger.Errorf("Failed to GC BadgerDB: %v", err)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

type collector struct {
	kv []*bpb.KV
}

func (c *collector) Send(list *bpb.KVList) error {
	c.kv = append(c.kv, list.Kv...)
	return nil
}

func (bdb *BadgerDB) Iterate(ctx context.Context) (map[url.URL]DocInfo, error) {
	stream := bdb.db.NewStream()
	stream.LogPrefix = "Iterating using Stream framework"

	c := &collector{}

	stream.Send = func(list *bpb.KVList) error {
		return c.Send(list)
	}

	err := stream.Orchestrate(ctx)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	ret := make(map[url.URL]DocInfo)
	for _, kv := range c.kv {
		tempURL := &url.URL{}
		if err = tempURL.UnmarshalBinary(kv.Key); err != nil {
			log.Fatal(err)
			return nil, err
		}

		var tempDocInfo DocInfo
		err = json.Unmarshal(kv.Value, &tempDocInfo)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		ret[*tempURL] = tempDocInfo
	}
	return ret, nil
}
