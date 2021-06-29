package RoseDB

import (
	"github.com/roseduan/rosedb/ds/set"
	"sync"
)

type SetIdx struct {
	mu      sync.RWMutex
	indexes *set.Set
}
