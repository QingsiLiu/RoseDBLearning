package storage

import (
	"github.com/roseduan/mmap-go"
	"os"
)

//文件读写数据的编码方式
type FileRWMethod uint8

type DBFile struct {
	Id     uint32
	path   string
	File   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod
}
