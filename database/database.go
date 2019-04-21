package database

import (
	"context"
	"fmt"
	"github.com/apsdehal/go-logger"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	bpb "github.com/dgraph-io/badger/pb"
	"github.com/pkg/errors"
	"os"
	"strconv"
	"time"
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
	// TODO: add logger debug in each function
	DB interface {
		// TODO: integrate prefix search

		// return nil if key not found
		Get(ctx context.Context, key interface{}) (value interface{}, err error)

		Set(ctx context.Context, key interface{}, value interface{}) error

		// return nil if key not found
		Has(ctx context.Context, key interface{}) (bool, error)

		// delete an key-value pair in the table, given the key
		Delete(ctx context.Context, key interface{}) error

		// will call cancel which aborts all process running on the ctx
		Close(ctx context.Context, cancel context.CancelFunc) error

		// DropTable will remove all data in a table (directory)
		DropTable(ctx context.Context) error

		// data is in random , due to concurrency
		Iterate(ctx context.Context) (*Collector, error)

		// initialise BadgerWriteBatch object for the corresponding table
		BatchWrite_init(ctx context.Context) BatchWriter

		// ONLY USE FOR DEBUGGING PURPOSES
		Debug_Print(ctx context.Context) error
	}

	BadgerDB struct {
		db      *badger.DB
		logger  *logger.Logger
		keyType string
		valType string
	}
)


type (
	BatchWriter interface {
		// initialise batch writer, set and collect the key-value pairs to be written in batch
		BatchSet(ctx context.Context, key interface{}, value interface{}) error

		// write the key value pairs collected in the batch writer
		// will return nothing if there is nothing to flush
		Flush(ctx context.Context) error

		// wrapper around Cancel function for deferring
		Cancel(ctx context.Context)
	}

	// wrapper around badger WriteBatch to support type checking
	BadgerBatchWriter struct {
		batchWriter *badger.WriteBatch
		keyType     string
		valType     string
	}
)

/*
	object passed on DB_init should be used as global variable, only call DB_init once (operation on database object can be concurrent)
refer to `noschema_schema.go` for each table's key and value data types
	\params: context, logger
	\return: list of inverted tables, list of forward tables (type: []DB), error
		inv[0]: inverted table for keywords in title section
		inv[1]: inverted table for keywords in body section
		forw[0]: forward table for wordHash (wordId) to word mapping
		forw[1]: forward table for docHash (docId) to DocInfo mapping
		forw[2]: forward table for docHash to list of its child
		forw[3]: forward table for docHash to pageRank value
*/

func DB_init(ctx context.Context, logger *logger.Logger) (inv []DB, forw []DB, err error) {
	base_dir := "./db_data/"

	// table loading mode
	// default is MemoryMap, 1 is LoadToRAM (most optimised), 2 is FileIO (all disk)
	loadMode := 1

	// directory of table is mapped to the configurations (table loading mode, key data type, and value data type). Data type is stored to support schema enforcement
	inverted := [][]string{
		[]string{"invKeyword_title/", strconv.Itoa(loadMode), "string", "map[string][]uint32"},
		[]string{"invKeyword_body/", strconv.Itoa(loadMode), "string", "map[string][]uint32"},
	}

	forward := [][]string{
		[]string{"WordHash_word/", strconv.Itoa(loadMode), "string", "string"},
		[]string{"DocHash_docInfo/", strconv.Itoa(loadMode), "string", "DocInfo"},
		[]string{"DocHash_children/", strconv.Itoa(loadMode), "string", "[]string"},
		[]string{"DocHash_rank/", strconv.Itoa(loadMode), "string", "float64"},
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

		temp, err := NewBadgerDB(ctx, base_dir+v[0], logger, tempMethod, v[2], v[3])
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

func NewBadgerDB(ctx context.Context, dir string, logger *logger.Logger, loadMethod int, keyType string, valType string) (DB, error) {
	opts := getOpts(loadMethod, dir)

	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	bdb := &BadgerDB{
		db:      badgerDB,
		logger:  logger,
		keyType: keyType,
		valType: valType,
	}

	// run garbage collection in advance
	go bdb.runGC(ctx)
	return bdb, nil
}

// helper function for ease of DB configurations tuning
func getOpts(loadMethod int, dir string) (opts badger.Options) {
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

func (bwb *BadgerBatchWriter) BatchSet(ctx context.Context, key_ interface{}, value_ interface{}) error {
	key, value, err := checkMarshal(key_, bwb.keyType, value_, bwb.valType)
	if err != nil {
		return err
	}

	// pass the key-value pairs in []byte to the batch writer
	if err = bwb.batchWriter.Set(key, value, 0); err != nil {
		return err
	}
	return nil
}

func (bwb *BadgerBatchWriter) Flush(ctx context.Context) error {
	if err := bwb.batchWriter.Flush(); err != nil {
		panic(err)
		return err
	}
	return nil
}

func (bwb *BadgerBatchWriter) Cancel(ctx context.Context) {
	bwb.batchWriter.Cancel()
}

func (bdb *BadgerDB) BatchWrite_init(ctx context.Context) BatchWriter {
	bwb := &BadgerBatchWriter{
		batchWriter: bdb.db.NewWriteBatch(),
		keyType:     bdb.keyType,
		valType:     bdb.valType,
	}

	return bwb
}

func (bdb *BadgerDB) DropTable(ctx context.Context) error {
	return bdb.db.DropAll()
}

func (bdb *BadgerDB) Get(ctx context.Context, key_ interface{}) (value_ interface{}, err error) {
	// key and value has type of []byte, for the passing to transactions
	var value []byte
	key, _, err := checkMarshal(key_, bdb.keyType, nil, "")
	if err != nil {
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

type Collector struct {
	KV []*bpb.KV
}

func (c *Collector) Send(list *bpb.KVList) error {
	c.KV = append(c.KV, list.Kv...)
	return nil
}

func (bdb *BadgerDB) Iterate(ctx context.Context) (*Collector, error) {
	stream := bdb.db.NewStream()
	stream.LogPrefix = "Iterating using Stream framework"

	c := &Collector{}

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
