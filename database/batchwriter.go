package database

import (
	"context"
	"github.com/dgraph-io/badger"
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

func (bwb *BadgerBatchWriter) BatchSet(ctx context.Context, key_ interface{}, value_ interface{}) error {
	key, value, err := checkMarshal(key_, bwb.keyType, value_, bwb.valType)
	if err != nil {
		return err
	}

	// pass the key-value pairs in []byte to the batch writer
	if err = bwb.batchWriter.Set(key, value); err != nil {
		return err
	}
	return nil
}
