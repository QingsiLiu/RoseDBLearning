package RoseDB

import (
	"github.com/roseduan/rosedb/ds/zset"
	"sync"
)

type ZsetIdx struct {
	mu      sync.RWMutex
	indexes *zset.SortedSet
}
