package RoseDB

import (
	"RoseDB/storage"
	"sync"
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
		isReclaiming       bool
		isSingleReclaiming bool
	}

	//不同数据类型的当前活动文件
	ActiveFiles map[DataType]*storage.DBFile

	//当前活动文件不同的数据类型id
	ActiveFileIds map[DataType]uint32

	//定义归档文件，这些文件只能读取,永远不会有写入操作
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile

	//保存不同密钥的过期信息
	Expires map[DataType]map[string]int64
)
