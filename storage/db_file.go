package storage

import (
	"errors"
	"fmt"
	"github.com/roseduan/mmap-go"
	"hash/crc32"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
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

	//表示db文件的后缀名
	DBFileSuffixName = []string{"str", "list", "hash", "set", "zset"}
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
	if buf, err = df.readBuf(offset, int64(entryHeaderSize)); err != nil {
		return
	}

	if e, err = Decode(buf); err != nil {
		return
	}

	//文件偏移量从头部向后，定位到Key的位置
	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}

	//文件偏移量向后，定位到Val的位置
	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var value []byte
		if value, err = df.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return
		}
		e.Meta.Value = value
	}

	//文件偏移量向后，定位到Extra的位置
	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var extra []byte
		if extra, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = extra
	}

	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc != e.crc32 {
		return nil, ErrInvalidCrc
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

//读写后将文件关闭; sync:关闭前是否持久化数据
func (df *DBFile) Close(sync bool) (err error) {
	if !sync {
		err = df.Sync()
	}

	if df.File != nil {
		err = df.File.Close()
	}
	if df.mmap != nil {
		err = df.mmap.Unmap()
	}
	return
}

//加载数据文件
func Build(path string, method FileRWMethod, blockSize int64) (map[uint16]map[uint32]*DBFile, map[uint16]uint32, error) {
	//ReadDir读取以dirname命名的目录，并返回fs列表。FileInfo用于目录的内容，按文件名排序。如果在读取目录时发生错误，ReadDir将不返回伴随错误的任何目录条目。
	//从Go 1.16开始，os。ReadDir是一个更有效和正确的选择:它返回fs的列表。用DirEntry代替fs。FileInfo，并且在读取目录的中途出错的情况下返回部分结果
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, nil, err
	}

	//获取数据文件的全部Id
	fileIdsMap := make(map[uint16][]int)
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") {
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])

			switch splitNames[2] {
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			case DBFileSuffixName[1]:
				fileIdsMap[1] = append(fileIdsMap[1], id)
			case DBFileSuffixName[2]:
				fileIdsMap[2] = append(fileIdsMap[2], id)
			case DBFileSuffixName[3]:
				fileIdsMap[3] = append(fileIdsMap[3], id)
			case DBFileSuffixName[4]:
				fileIdsMap[4] = append(fileIdsMap[4], id)
			}
		}
	}

	//加载全部的数据文件
	activeFileIds := make(map[uint16]uint32)
	archFiles := make(map[uint16]map[uint32]*DBFile)
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ {
		fileIds := fileIdsMap[dataType]
		sort.Ints(fileIds)
		files := make(map[uint32]*DBFile)
		var activeFileId uint32 = 0

		if len(fileIds) > 0 {
			activeFileId = uint32(fileIds[len(fileIds)-1])

			for i := 0; i < len(fileIds)-1; i++ {
				id := fileIds[i]
				file, err := NewDBFile(path, uint32(id), method, blockSize, dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file
			}
		}

		archFiles[dataType] = files
		activeFileIds[dataType] = activeFileId
	}
	return archFiles, activeFileIds, nil
}
