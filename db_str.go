package RoseDB

import (
	"github.com/roseduan/rosedb/index"
	"sync"
)

type StrIdx struct {
	mu      sync.RWMutex
	idxList *index.SkipList
}

func (db *RoseDB) Set(key, value []byte) error {
	return db.doSet(key, value)
}

func (db *RoseDB) doSet(key, value []byte) (err error) {

	return
}
