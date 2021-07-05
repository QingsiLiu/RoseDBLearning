package RoseDB

import (
	"RoseDB/index"
	"RoseDB/storage"
	"RoseDB/utils"
	"errors"
	"log"
	"os"
	"sync"
	"time"
)

var (
	// ErrEmptyKey 键值为空
	ErrEmptyKey = errors.New("rosedb: the key is empty")

	// ErrKeyNotExist 键key不存在
	ErrKeyNotExist = errors.New("rosedb: key not exist")

	// ErrKeyTooLarge 键值太大
	ErrKeyTooLarge = errors.New("rosedb: key exceeded the max length")

	// ErrValueTooLarge 值val太大
	ErrValueTooLarge = errors.New("rosedb: value exceeded the max length")

	// ErrNilIndexer 索引为空
	ErrNilIndexer = errors.New("rosedb: indexer is nil")

	// ErrCfgNotExist 配置不存在
	ErrCfgNotExist = errors.New("rosedb: the config file not exist")

	// ErrReclaimUnreached 还没有准备好收回
	ErrReclaimUnreached = errors.New("rosedb: unused space not reach the threshold")

	// ErrExtraContainsSeparator 额外包含分隔符
	ErrExtraContainsSeparator = errors.New("rosedb: extra contains separator \\0")

	// ErrInvalidTTL ttl是无效的
	ErrInvalidTTL = errors.New("rosedb: invalid ttl")

	// ErrKeyExpired 键值已过期
	ErrKeyExpired = errors.New("rosedb: key is expired")

	// ErrDBisReclaiming 回收和单个回收不能同时执行
	ErrDBisReclaiming = errors.New("rosedb: can`t do reclaim and single reclaim at the same time")
)

const (

	// rosedb配置文件的保存路径
	configSaveFile = string(os.PathSeparator) + "DB.CFG"

	// 保存rosedb元信息的路径
	dbMetaSaveFile = string(os.PathSeparator) + "DB.META"

	// Rosedb回收路径(一个临时目录)将在回收后被删除。
	reclaimPath = string(os.PathSeparator) + "rosedb_reclaim"

	// 分隔符的额外信息，一些命令不能包含它
	ExtraSeparator = "\\0"

	// 不同数据结构的数量(string, list, hash, set, zset).
	DataStructureNum = 5
)

type (
	RoseDB struct {
		//这个结构体表示一个db实例
		activeFile         ActiveFiles     // 当前活动的文件
		activeFileIds      ActiveFileIds   // 当前活动文件id
		archFiles          ArchivedFiles   // 归档文件
		strIndex           *StrIdx         // 字符串索引(一个跳表)
		listIndex          *ListIdx        // 列表索引
		hashIndex          *HashIdx        // 哈希索引
		setIndex           *SetIdx         // Set集合索引
		zsetIndex          *ZsetIdx        // 顺序set集合索引
		config             Config          // rosedb的配置信息
		mu                 sync.RWMutex    // 互斥锁
		meta               *storage.DBMeta // 元信息的rosedb
		expires            Expires         // 过期目录
		isReclaiming       bool            //是否回收
		isSingleReclaiming bool            //是否单回收
	}

	//不同数据类型的当前活动文件
	ActiveFiles map[DataType]*storage.DBFile

	//当前活动文件不同的数据类型id
	ActiveFileIds map[DataType]uint32

	//定义归档文件，这些文件只能读取,永远不会有写入操作
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile

	//保存不同key键值的过期信息(根据数据类型以及key键值来获得deadline)
	Expires map[DataType]map[string]int64
)

//打开一个RoseDB数据库实例
func Open(config Config) (*RoseDB, error) {
	//校检文件的路径是否存在，如果不存在则创建文件
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//从磁盘中加载文件
	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	//将活跃文件activefiles实例化为可写文件
	activeFiles := make(ActiveFiles)
	for dataType, fileid := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileid, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles[dataType] = file
	}

	//加载数据库meta元信息，写偏移量只在activefile上
	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	for dataType, file := range activeFiles {
		file.Offset = meta.ActiveWriteOff[dataType]
	}

	db := &RoseDB{
		activeFile:    activeFiles,
		activeFileIds: activeFileIds,
		archFiles:     archFiles,
		config:        config,
		strIndex:      newStrIdx(),
		meta:          meta,
		listIndex:     newListIdx(),
		expires:       make(Expires),
	}
	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}

	//加载索引信息
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

//为不同的数据结构构建索引
func (db *RoseDB) buildIndex(entry *storage.Entry, idx *index.Indexer) error {
	if db.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = entry.Meta.ValueSize
	}

	switch entry.GetType() {
	case storage.String:
		db.buildStringIndex(idx, entry)
	}
	return nil
}

//将entry数据写入文件中
func (db *RoseDB) store(e *storage.Entry) error {
	//检查文件大小，如果文件大小不够，同步数据库文件，并打开一个新的数据库文件
	config := db.config
	if db.activeFile[e.GetType()].Offset+int64(e.Size()) > config.BlockSize {
		//将当前的活跃文件块进行数据持久化
		if err := db.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}

		//将老的活跃db文件转化为已归档文件
		activeFileId := db.activeFileIds[e.GetType()]
		db.archFiles[e.GetType()][activeFileId] = db.activeFile[e.GetType()]
		activeFileId = activeFileId + 1

		//创建一个新的DBfile数据文件
		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil {
			return err
		}

		db.activeFile[e.GetType()] = newDbFile
		db.activeFileIds[e.GetType()] = activeFileId
		db.meta.ActiveWriteOff[e.GetType()] = 0
	}

	//将entry数据块写入dbfile中
	if err := db.activeFile[e.GetType()].Write(e); err != nil {
		return err
	}

	//更新文件的偏移量
	db.meta.ActiveWriteOff[e.GetType()] = db.activeFile[e.GetType()].Offset

	//根据配置保存db文件
	if config.Sync {
		if err := db.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}
	}

	return nil
}

//校检检验key/val的合法性
func (db *RoseDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))

	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	return nil
}

//检查key是否已经过期，并且可以进行删除
func (db *RoseDB) checkExpires(key []byte, dType DataType) (expired bool) {
	deadline, exists := db.expires[dType][string(key)]
	if !exists {
		return
	}

	//如果已经过期
	if time.Now().Unix() > deadline {
		expired = true

		//删除已经过期的key键值
		var e *storage.Entry
		switch dType {
		case String:
			e = storage.NewEntryNoExtra(key, nil, String, StringRem)
			if ele := db.strIndex.idxList.Remove(key); ele != nil {
				db.incrReclaimableSpace(key)
			}
		}

		if err := db.store(e); err != nil {
			log.Println("checkExpired: store entry err: ", err)
			return
		}

		//删除过期的key文件
		delete(db.expires[dType], string(key))
	}
	return
}
