package storage

//保存一些额外的rosedb信息，以后可能会添加其他配置
type DBMeta struct {
	ActiveWriteOff   map[uint16]int64 `json:"active_write_off"`  // 当前活动db文件的写偏移量
	ReclaimableSpace map[uint32]int64 `json:"reclaimable_space"` // 每个db文件中的可回收空间，用于单个回收
}
