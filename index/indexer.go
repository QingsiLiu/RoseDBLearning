package index

import "RoseDB/storage"

//数据索引信息，存储在跳跃列表中
type Indexer struct {
	Meta      *storage.Meta
	FileId    uint32
	EntrySize uint32
	Offset    int64
}
