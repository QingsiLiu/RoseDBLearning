package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

//保存一些额外的rosedb信息，以后可能会添加其他配置
type DBMeta struct {
	ActiveWriteOff   map[uint16]int64 `json:"active_write_off"`  // 当前活动db文件的写偏移量
	ReclaimableSpace map[uint32]int64 `json:"reclaimable_space"` // 每个db文件中的可回收空间，用于单个回收
}

//从数据库文件中加载Meta元文件
func LoadMeta(path string) (m *DBMeta) {
	m = &DBMeta{
		ActiveWriteOff:   make(map[uint16]int64),
		ReclaimableSpace: make(map[uint32]int64),
	}

	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	_ = json.Unmarshal(b, m)
	return
}
