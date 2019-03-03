package main

import (
	"fmt"
	"log"
	"github.com/dgraph-io/badger"
)

func main() {
	// testing db
	tmpDir := "/home/asus/go/tmp/badger/"
	
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
