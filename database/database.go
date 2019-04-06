package database

import (
	"context"
	"fmt"
	"github.com/apsdehal/go-logger"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/pkg/errors"
	bpb "github.com/dgraph-io/badger/pb"
	"os"
	"time"
	"strconv"
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
	
	ErrKeyTypeNotMatch = errors.New("Invalid key type, key type must match with the schema defined! Refer to database.go or noschema_schema.go for schema documentation.")
	
	ErrValTypeNotMatch = errors.New("Invalid value type, value type must follows with the schema defined! Refer to database.go or noschema_schema.go for schema documentation.")

	ErrKeyTypeNotFound = errors.New("Key type not found, double check the type variable passed")
	
	ErrValTypeNotFound = errors.New("Value type not found, double check the type variable passed")
)

type (
	// Set method should be passed InvKeyword_values as the underlying datatype of the value
	// AppendValue method should be passed InvKeyword_value as the underlying datatype of the appended value

	DB_Inverted interface {
		DB
		AppendValue(ctx context.Context, key interface{}, appendedValue interface{}) error
	}

	BadgerDB_Inverted struct {
		BadgerDB
	}
)

type (
	// TODO: add logger debug in each function
	DB interface {
		// TODO: integrate prefix search

		// return nil if key not found
		Get(ctx context.Context, key interface{}) (value interface{}, err error)

		Set(ctx context.Context, key interface{}, value interface{}) error

		// return nil if key not found
		Has(ctx context.Context, key interface{})(bool, error)

		Delete(ctx context.Context, key interface{}) error

		// will call cancel which aborts all process running on the ctx
		Close(ctx context.Context, cancel context.CancelFunc) error

		// DropTable will remove all data in a table (directory)
		DropTable(ctx context.Context) error

		// data is in random , due to concurrency
		Iterate(ctx context.Context) (*collector, error)

		// batch write api to minimise the creation of transaction
		BatchWrite_init(ctx context.Context) *badger.WriteBatch

		// ONLY USE FOR DEBUGGING PURPOSES
		Debug_Print(ctx context.Context) error
	}

	BadgerDB struct {
		db     *badger.DB
		logger *logger.Logger
		keyType	string
		valType	string
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
		forw[2]: forward table for URL to DocId mapping
		forw[3]: forward table for DocId to DocInfo mapping
		forw[4]: forward table for keeping track of index
*/

func DB_init(ctx context.Context, logger *logger.Logger) (inv []DB_Inverted, forw []DB, err error) {
	base_dir := "./db_data/"
	
	// table loading mode
	// default is MemoryMap, 1 is LoadToRAM (most optimised), 2 is FileIO (all disk)
	temp := 1

	// directory of table is mapped to the configurations (table loading mode, key data type, and value data type). Data type is stored to support schema enforcement	
	inverted := [][]string{
		[]string{"invKeyword_body/", strconv.Itoa(temp), "uint32", "map[uint16][]uint32"},
		[]string{"invKeyword_title/", strconv.Itoa(temp), "uint32", "map[uint16][]uint32"},
	}

	forward := [][]string{
		[]string{"Word_wordId/", strconv.Itoa(temp), "string", "uint32"}, 
		[]string{"WordId_word/", strconv.Itoa(temp), "uint32", "string"}, 
		[]string{"URL_docId/", strconv.Itoa(temp), "url.URL", "uint16"},
		[]string{"DocId_docInfo/", strconv.Itoa(temp), "uint16", "DocInfo"},
		[]string{"Indexes/", strconv.Itoa(temp), "string", "uint32"},
	}

	// create directory if not exist
	for _, d := range inverted {
		if _, err := os.Stat(base_dir + d[0]); os.IsNotExist(err) {
			os.MkdirAll(base_dir+d[0], 0755)
		}
	}

	for _, d := range forward {
		if _, err := os.Stat(base_dir + d[0]); os.IsNotExist(err) {
			os.MkdirAll(base_dir+d[0], 0755)
		}
	}

	// initiate table object
	for _, v := range inverted {
		// get the table loading method first
		tempMethod, err := strconv.Atoi(v[1])
		if err != nil {
			return nil, nil, err
		}

		temp, err := NewBadgerDB_Inverted(ctx, base_dir+v[0], logger, tempMethod, v[2], v[3])
		if err != nil {
			return nil, nil, err
		}
		inv = append(inv, temp)
	}

	for _, v := range forward {
		// get the table loading method first
		tempMethod, err := strconv.Atoi(v[1])
		if err != nil {
			return nil, nil, err
		}

		temp, err := NewBadgerDB(ctx, base_dir+v[0], logger, tempMethod, v[2], v[3])
		if err != nil {
			return nil, nil, err
		}
		forw = append(forw, temp)
	}

	return inv, forw, nil
}


func NewBadgerDB_Inverted(ctx context.Context, dir string, logger *logger.Logger, loadMethod int, keyType string, valType string) (DB_Inverted, error) {
	opts := getOpts(loadMethod, dir)

	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	bdb_i := &BadgerDB_Inverted{BadgerDB{badgerDB, logger, keyType, valType,}}

	// run garbage collection in advance
	go bdb_i.runGC(ctx)
	return bdb_i, nil
}


func NewBadgerDB(ctx context.Context, dir string, logger *logger.Logger, loadMethod int, keyType string, valType string) (DB, error) {
	opts := getOpts(loadMethod, dir)

	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	bdb := &BadgerDB{
		db:     badgerDB,
		logger: logger,
		keyType: keyType,
		valType: valType,
	}

	// run garbage collection in advance
	go bdb.runGC(ctx)
	return bdb, nil
}


// helper function for ease of DB configurations tuning
func getOpts(loadMethod int, dir string)(opts badger.Options){
	opts = badger.DefaultOptions
	opts.Dir, opts.ValueDir = dir, dir

	// if false, SyncWrites write into tables in RAM, write to disk when full. Increase performance but may cause loss of data
	opts.SyncWrites = true

	// loadMethod: default is MemoryMap, 1 for loading to memory (LoadToRAM), 2 for storing all into disk (FileIO) which resulted in extensive disk IO
	switch loadMethod {
		case 1: 
			opts.TableLoadingMode = options.LoadToRAM
		case 2:
			opts.TableLoadingMode, opts.ValueLogLoadingMode = options.FileIO, options.FileIO
	}
	return opts
}


func (bdb *BadgerDB) BatchWrite_init(ctx context.Context) *badger.WriteBatch{
	return bdb.db.NewWriteBatch()
}


func (bdb *BadgerDB) DropTable(ctx context.Context) error {
	return bdb.db.DropAll()
}


func (bdb_i *BadgerDB_Inverted) AppendValue(ctx context.Context, key interface{}, appendedValue interface{}) error {
	if _, _, err := checkMarshal(key, bdb_i.keyType, appendedValue, bdb_i.valType); err != nil { 
		return err 
	}
	
	// value has type of map[uint16][]uint32
	value, err := bdb_i.Get(ctx, key)
	if err != nil {	
		return err 
	}

	// append the appendedValue into 
	for k, v := range appendedValue.(map[uint16][]uint32) {
		value.(map[uint16][]uint32)[k] = v
	}

	// delete and set the new appended values
	if err = bdb_i.Set(ctx, key, value); err != nil {
		return err
	}
	return nil
}


func (bdb *BadgerDB) Get(ctx context.Context, key_ interface{}) (value_ interface{}, err error){
	// key and value has type of []byte, for the passing to transactions
	var value []byte
	key, _, err := checkMarshal(key_, bdb.keyType, nil, "")
	if err != nil {
		fmt.Println("error from get ", err)
		return nil, err
	}

	err = bdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)

		if err != nil {
			return err
		}

		// value needed to be copied as it only lasts when the transaction is open
		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})

		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	
	value_, err = checkUnmarshal(value, bdb.valType)
	if err != nil {
		return nil, err
	} 

	return
}


func (bdb *BadgerDB) Set(ctx context.Context, key_ interface{}, value_ interface{}) error {
	key, value, err := checkMarshal(key_, bdb.keyType, value_, bdb.valType)
	if err != nil { 
		return err 
	}

	err = bdb.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})

	if err != nil {
		bdb.logger.Debugf("Failed to set key %s: %v", key, value)
		return err
	}
	return nil
}


func (bdb *BadgerDB) Has(ctx context.Context, key_ interface{}) (ok bool, err error) {
	_, _, err = checkMarshal(key_, bdb.keyType, nil, "")
	if err != nil { 
		return false, err 
	}

	_, err = bdb.Get(ctx, key_)
	switch err {
	case badger.ErrKeyNotFound:
		return false, nil
	case nil:
		return true, nil
	}
	return 
}


func (bdb *BadgerDB) Delete(ctx context.Context, key_ interface{}) error {
	key, _, err := checkMarshal(key_, bdb.keyType, nil, "")
	if err != nil { 
		return err
	}

	err = bdb.db.Update(func(txn *badger.Txn) error {
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
	KV []*bpb.KV
}


func (c *collector) Send(list *bpb.KVList) error {
	c.KV = append(c.KV, list.Kv...)
	return nil
}


func (bdb *BadgerDB) Iterate(ctx context.Context) (*collector, error) {
	stream := bdb.db.NewStream()
	stream.LogPrefix = "Iterating using Stream framework"

	c := &collector{}

	stream.Send = func(list *bpb.KVList) error {
		return c.Send(list)
	}

	err := stream.Orchestrate(ctx)
	if err != nil {
		return nil, err
	}
	return c, nil
}


func (bdb *BadgerDB) Debug_Print(ctx context.Context) error {
	err := bdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("\tkey=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}
