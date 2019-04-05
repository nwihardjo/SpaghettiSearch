package database

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apsdehal/go-logger"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	bpb "github.com/dgraph-io/badger/pb"
	"os"
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

		// return nil if key not found
		Get(ctx context.Context, key []byte) (value []byte, err error)

		Set(ctx context.Context, key []byte, value []byte) error

		// return nil if key not found
		Has(ctx context.Context, key []byte) (bool, error)

		Delete(ctx context.Context, key []byte) error

		// will call cancel which aborts all process running on the ctx
		Close(ctx context.Context, cancel context.CancelFunc) error

		// DropTable will remove all data in a table (directory)
		DropTable(ctx context.Context) error

		// data is in random , due to concurrency
		Iterate(ctx context.Context) (*collector, error)

		// batch write api to minimise the creation of transaction
		//BatchSet(ctx context.Context, key []byte, keyType string, value []byte, valueType string) error

		BatchWrite_init(ctx context.Context) *badger.WriteBatch
		// ONLY USE FOR DEBUGGING PURPOSES
		Debug_Print(ctx context.Context) error
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
		forw[2]: forward table for URL to DocId mapping
		forw[3]: forward table for DocId to DocInfo mapping
		forw[4]: forward table for keeping track of index
*/

func DB_init(ctx context.Context, logger *logger.Logger) (inv []DB_Inverted, forw []DB, err error) {
	base_dir := "./db_data/"
	temp := 2

	inverted_dirs := []string{"invKeyword_body/", "invKeyword_title/"}
	inverted_modes := []int{temp, temp}
	forward_dirs := []string{"Word_wordId/", "WordId_word", "URL_docId/", "DocId_docInfo/", "Indexes/"}
	forward_modes := []int{temp, temp, temp, temp, temp}

	// create directory if not exist
	for _, d := range inverted_dirs {
		if _, err := os.Stat(base_dir + d); os.IsNotExist(err) {
			os.MkdirAll(base_dir+d, 0755)
		}
	}

	for _, d := range forward_dirs {
		if _, err := os.Stat(base_dir + d); os.IsNotExist(err) {
			os.MkdirAll(base_dir+d, 0755)
		}
	}

	// initiate table object
	for i, v := range inverted_dirs {
		temp, err := NewBadgerDB_Inverted(ctx, base_dir+v, logger, inverted_modes[i])
		if err != nil {
			return nil, nil, err
		}
		inv = append(inv, temp)
	}

	for i, v := range forward_dirs {
		temp, err := NewBadgerDB(ctx, base_dir+v, logger, forward_modes[i])
		if err != nil {
			return nil, nil, err
		}
		forw = append(forw, temp)
	}

	return inv, forw, nil
}

func (bdb *BadgerDB) BatchWrite_init(ctx context.Context) *badger.WriteBatch {
	return bdb.db.NewWriteBatch()
}

func NewBadgerDB_Inverted(ctx context.Context, dir string, logger *logger.Logger, loadIntoRAM int) (DB_Inverted, error) {
	opts := badger.DefaultOptions
	opts.Dir, opts.ValueDir = dir, dir

	// 0 is the default options, which uses MemoryMap for both TableLoadingMode and ValueLogLoadingMode, already defined on DefaultOptions
	// 1 is LoadToRam on TableLoadingMode, the most optimised. ValueLoadingMode can't be load into RAM
	// 2 for store everything in disk, require extensive Disk
	if loadIntoRAM == 1 {
		// How should LSM tree be accessed
		opts.TableLoadingMode = options.LoadToRAM
	} else if loadIntoRAM == 2 {
		opts.TableLoadingMode = options.FileIO
		opts.ValueLogLoadingMode = options.FileIO
	}

	opts.SyncWrites = false

	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	bdb_i := &BadgerDB_Inverted{BadgerDB{badgerDB, logger}}

	// run garbage collection in advance
	go bdb_i.runGC(ctx)
	return bdb_i, nil
}

func NewBadgerDB(ctx context.Context, dir string, logger *logger.Logger, loadIntoRAM int) (DB, error) {
	opts := badger.DefaultOptions
	opts.Dir, opts.ValueDir = dir, dir

	// 0 is the default options, which uses MemoryMap for both TableLoadingMode and ValueLogLoadingMode, already defined on DefaultOptions
	// 1 is LoadToRam on TableLoadingMode, the most optimised. ValueLoadingMode can't be load into RAM
	// 2 for store everything in disk, require extensive Disk
	if loadIntoRAM == 1 {
		// How should LSM tree be accessed
		opts.TableLoadingMode = options.LoadToRAM
	} else if loadIntoRAM == 2 {
		opts.TableLoadingMode = options.FileIO
		opts.ValueLogLoadingMode = options.FileIO
	}

	// set SyncWrites to False for performance increase but may cause loss of data
	opts.SyncWrites = false

	badgerDB, err := badger.Open(opts)
	if err != nil {
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
		return err
	}

	var appendedValue_struct InvKeyword_value
	var tempValues InvKeyword_values
	err = json.Unmarshal(value, &tempValues)
	if err != nil {
		return err
	}
	err = json.Unmarshal(appendedValue, &appendedValue_struct)
	if err != nil {
		return err
	}

	tempValues = append(tempValues, appendedValue_struct)
	tempVal, err := json.Marshal(tempValues)
	if err != nil {
		return err
	}

	// delete and set the new appended values
	// TODO: optimise the operation
	if err = bdb_i.Delete(ctx, key); err != nil {
		return err
	}
	if err = bdb_i.Set(ctx, key, tempVal); err != nil {
		return err
	}
	return nil
}

func (bdb *BadgerDB) Get(ctx context.Context, key []byte) (value []byte, err error) {
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
		// other error
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
