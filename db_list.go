package RoseDB

import (
	"container/list"
	"sync"
)

type ListIdx struct {
	mu      sync.RWMutex
	indexes *list.List
}
