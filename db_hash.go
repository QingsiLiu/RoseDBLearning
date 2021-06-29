package RoseDB

import (
	"hash"
	"sync"
)

type HashIdx struct {
	mu      sync.RWMutex
	indexes *hash.Hash
}
