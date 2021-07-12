package RoseDB

import (
	"RoseDB/index"
	"RoseDB/storage"
	"bytes"
	"sync"
)

type StrIdx struct {
	mu      sync.RWMutex
	idxList *index.SkipList
}

func newStrIdx() *StrIdx {
	return &StrIdx{idxList: index.NewSkipList()}
}

//设置key来保存字符串值。如果key已经保存了一个值val，那么会将被之前的val值覆盖
func (db *RoseDB) Set(key, value []byte) error {
	return db.doSet(key, value)
}

func (db *RoseDB) doSet(key, value []byte) (err error) {
	//校验合法性
	if err = db.checkKeyValue(key, value); err != nil {
		return err
	}

	//如果存在的值与设置的值相同，则不做任何操作
	if db.config.IdxMode == KeyValueMemMode {
		if existVal, _ := db.Get(key); existVal != nil && bytes.Compare(existVal, value) == 0 {
			return
		}
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, value, String, StringSet)
	if err := db.store(e); err != nil {
		return err
	}

	db.incrReclaimableSpace(key)
	// clear expire time.
	delete(db.expires[String], string(key))

	// string indexes, stored in skiplist.
	idx := &index.Indexer{
		Meta: &storage.Meta{
			KeySize:   uint32(len(e.Meta.Key)),
			Key:       e.Meta.Key,
			ValueSize: uint32(len(e.Meta.Value)),
		},
		FileId:    db.activeFileIds[String],
		EntrySize: e.Size(),
		Offset:    db.activeFile[String].Offset - int64(e.Size()),
	}
	db.strIndex.idxList.Put(idx.Meta.Key, idx)
	return
}

//获取原始索引信息并将可回收空间添加到db文件
func (db *RoseDB) incrReclaimableSpace(key []byte) {
	oldIdx := db.strIndex.idxList.Get(key)
	if oldIdx != nil {
		indexer := oldIdx.Value().(*index.Indexer)

		if indexer != nil {
			space := int64(indexer.EntrySize)
			db.meta.ReclaimableSpace[indexer.FileId] += space
		}
	}
}

//根绝key查询数据库中的val值,如果不存在key值则返回错误信息
func (db *RoseDB) Get(key []byte) ([]byte, error) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	//从内存的跳表中查询是否存在key键值的索引信息
	node := db.strIndex.idxList.Get(key)
	if node == nil {
		return nil, ErrKeyNotExist
	}

	idx := node.Value().(*index.Indexer)
	if idx == nil {
		return nil, ErrNilIndexer
	}

	//锁定RW操作读取过程
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RLock()

	//检查键值key是否已经过期
	if db.checkExpires(key, String) {
		return nil, ErrKeyExpired
	}

	//通过索引来获取加载在内存中的val值
	if db.config.IdxMode == KeyValueMemMode {
		return idx.Meta.Value, nil
	}

	//只有key在内存中时，val不在内存中，从文件中通过偏移量来获得val值
	if db.config.IdxMode == KeyOnlyMemMode {
		df := db.activeFile[String]

		if idx.FileId != db.activeFileIds[String] {
			df = db.archFiles[String][idx.FileId]
		}

		e, err := df.Read(idx.Offset)
		if err != nil {
			return nil, err
		}
		return e.Meta.Value, nil
	}

	return nil, ErrKeyNotExist
}
