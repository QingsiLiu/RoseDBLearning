package RoseDB

import "RoseDB/storage"

// DataIndexMode 数据索引的模式
type DataIndexMode int

const (
	//key和val都存于内存中的模式，读取效率很高，适用于规模较小的场景
	KeyValueMemMode DataIndexMode = iota
	//只有key存于内存中的模式
	KeyOnlyMemMode
)

//数据库配置
type Config struct {
	Addr                   string               `json:"addr" toml:"addr"`             //服务器地址          server address
	DirPath                string               `json:"dir_path" toml:"dir_path"`     //数据库数据存储目录   rosedb dir path of db file
	BlockSize              int64                `json:"block_size" toml:"block_size"` //每个数据块文件的大小 each db file size
	RwMethod               storage.FileRWMethod `json:"rw_method" toml:"rw_method"`   //数据读写模式        db file read and write method
	IdxMode                DataIndexMode        `json:"idx_mode" toml:"idx_mode"`     //数据索引模式        data index mode
	MaxKeySize             uint32               `json:"max_key_size" toml:"max_key_size"`
	MaxValueSize           uint32               `json:"max_value_size" toml:"max_value_size"`
	Sync                   bool                 `json:"sync" toml:"sync"`                           //每次写数据是否持久化 sync to disk
	ReclaimThreshold       int                  `json:"reclaim_threshold" toml:"reclaim_threshold"` //回收磁盘空间的阈值   threshold to reclaim disk
	SingleReclaimThreshold int64                `json:"single_reclaim_threshold"`                   // single reclaim threshold
}
