package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

var (
	// ErrInvalidEntry entry数据块不合法
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")
	// ErrInvalidCrc crc校检不合法
	ErrInvalidCrc = errors.New("storage/entry: invalid crc")
)

const (
	// KeySize, ValueSize, ExtraSize, crc32是uint32类型，每个4字节。
	// Timestamp占用8字节，state占用2字节。
	// 4 * 4 + 8 + 2 = 26
	entryHeaderSize = 26
)

// 数据结构类型的值
const (
	String uint16 = iota
	List
	Hash
	Set
	ZSet
)

type (
	//这是一个将被追加到db文件中的记录
	Entry struct {
		Meta      *Meta
		state     uint16 //State表示两个字段，高8位表示数据类型，低8位表示操作标记
		crc32     uint32 //校检和
		Timestamp uint64 //写入记录的时间戳
	}

	//元信息,写进数据库的基本信息
	Meta struct {
		Key       []byte
		Value     []byte
		Extra     []byte //操作入口的额外信息
		KeySize   uint32
		ValueSize uint32
		ExtraSize uint32
	}
)

//实例化Entry结构体实体
func newInternal(key, value, extra []byte, state uint16, timestamp uint64) *Entry {
	return &Entry{
		Meta: &Meta{
			Key:       key,
			Value:     value,
			Extra:     extra,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			ExtraSize: uint32(len(extra)),
		},
		state:     state,
		Timestamp: timestamp,
	}
}

//设置数据类型及操作类型并进行实例化结构体
func NewEntry(key, val, extra []byte, t, mark uint16) *Entry {
	var state uint16 = 0
	//设置数据类型以及操作类型
	state = state | (t << 8)
	state = state | mark
	return newInternal(key, val, extra, state, uint64(time.Now().Unix()))
}

//生成一个没有额外信息的entry数据结构体
func NewEntryNoExtra(key, value []byte, t, mark uint16) *Entry {
	return NewEntry(key, value, nil, t, mark)
}

//获取entry的文件数据类型（state的高8位）
func (e *Entry) GetType() uint16 {
	return e.state >> 8
}

//获取entry的操作数据类型（state的低8位）
func (e *Entry) GetMark() uint16 {
	return e.state & (2<<7 - 1)
}

//返回entry的总大小
func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.ExtraSize
}

//对数据块中的内容进行编码，并最后返回一个byte类型的数组
func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.Meta.KeySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	es := e.Meta.ExtraSize
	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	binary.BigEndian.PutUint32(buf[12:16], es)
	binary.BigEndian.PutUint16(buf[16:18], e.state)
	binary.BigEndian.PutUint64(buf[18:26], e.Timestamp)
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)
	copy(buf[entryHeaderSize+ks:(entryHeaderSize+ks+vs)], e.Meta.Value)
	if es > 0 {
		copy(buf[(entryHeaderSize+ks+vs):(entryHeaderSize+ks+vs+es)], e.Meta.Extra)
	}

	crc := crc32.ChecksumIEEE(e.Meta.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

//解码字节数组并返回一个entry数据集合
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	es := binary.BigEndian.Uint32(buf[12:16])
	state := binary.BigEndian.Uint16(buf[16:18])
	timestamp := binary.BigEndian.Uint64(buf[18:26])
	crc := binary.BigEndian.Uint32(buf[0:4])

	return &Entry{
		Meta: &Meta{
			KeySize:   ks,
			ValueSize: vs,
			ExtraSize: es,
		},
		state:     state,
		crc32:     crc,
		Timestamp: timestamp,
	}, nil
}
