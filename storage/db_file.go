package storage

import (
	"errors"
	"fmt"
	"github.com/roseduan/mmap-go"
	"os"
)

const (
	// FilePerm 默认的创建文件权限
	FilePerm = 0644
	//默认的路径分隔符
	PathSeparator = string(os.PathSeparator)
)

var (
	//默认数据文件名称的格式化
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}
)

var (
	// ErrEmptyEntry 数据块内容为空
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
)

//文件读写数据的编码方式
type FileRWMethod uint8

const (
	//FileIO 表示文件读写使用系统标准IO
	FileIO FileRWMethod = iota

	// MMap 表示文件数据读写使用Mmap,指的是将文件或其他设备映射至内存
	MMap
)

type DBFile struct {
	Id     uint32       //文章id
	path   string       //文章路径
	File   *os.File     //文章信息
	mmap   mmap.MMap    //表示映射到内存中的文件
	Offset int64        //偏离地址
	method FileRWMethod //编码方式
}

/*
必选项:以下三个常数中必须指定一个,且仅允许指定一个。
O_RDONLY 只读打开
O_WRONLY 只写打开
O_RDWR  可读可写打开
以下可选项可以同时指定0个或多个,和必选项按位或起来作为flags 参数。可选项有很多,这
里只介绍一部分,其它选项可参考open(2)的Man Page:
O_APPEND   表示追加。如果文件已有内容,这次打开文件所写的数据附加到文件的末尾而不
                     覆盖原来的内容。
O_CREATE 若此文件不存在则创建它。使用此选项时需要提供第三个参数mode ,表示该文件
                 的访问权限。
O_EXCL 如果同时指定了O_CREAT,并且文件已存在,则出错返回。
O_TRUNC  如果文件已存在,并且以只写或可读可写方式打开,则将其长度截断
                   (Truncate)为0字节。
O_NONBLOCK 对于设备文件,以O_NONBLOCK 方式打开可以做非阻塞I/O(Nonblock I/O)
*/
//实例化DBFile
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64, eType uint16) (*DBFile, error) {
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[eType], fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm) //如果为空则创建，并赋予可读写权限
	if err != nil {
		return nil, err
	}

	df := &DBFile{Id: fileId, path: path, Offset: 0, method: method}

	if method == FileIO {
		df.File = file
	} else {
		//更改文件的大小但不更改偏移量
		if err = file.Truncate(blockSize); err != nil {
			return nil, err
		}
		m, err := mmap.Map(file, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		df.mmap = m
	}
	return df, nil
}

//从文件中读数据，offset是起始位置
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte
	if buf, err := df.readBuf(offset, int64(entryHeaderSize)); err != nil {
		return
	}

	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}

	return
}

func (df *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)

	if df.method == FileIO {
		_, err := df.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}

	if df.method == MMap || offset <= int64(len(df.mmap)) {
		copy(buf, df.mmap[offset:])
	}
	return buf, nil
}

//从文件的offset处开始写数据
func (df *DBFile) Write(e *Entry) error {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method := df.method
	writeOff := df.Offset
	encVal, err := e.Encode()
	if err != nil {
		return err
	}

	if method == FileIO {
		if _, err := df.File.WriteAt(encVal, writeOff); err != nil {
			return err
		}
	}

	if method == MMap {
		copy(df.mmap[writeOff:], encVal)
	}
	df.Offset += int64(e.Size())

	return nil
}

//数据文件持久化
func (df *DBFile) Sync() (err error) {
	if df.File != nil {
		err = df.File.Sync()
	}

	if df.mmap != nil {
		err = df.mmap.Flush()
	}
	return
}
