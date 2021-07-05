package RoseDB

import (
	"RoseDB/index"
	"RoseDB/storage"
	"io"
	"log"
	"sort"
	"sync"
)

//定义数据结构类型
type DataType = uint16

//定义不同的数据类型
const (
	String DataType = iota
)

//定义String类型的操作
const (
	StringSet    uint16 = iota //添加设置string
	StringRem                  //删除string
	StringExpire               //string过期
)

//为字符串建立索引
func (db *RoseDB) buildStringIndex(idx *index.Indexer, entry *storage.Entry) {
	if db.listIndex == nil || idx == nil {
		return
	}

}

//从文件中加载索引信息
func (db *RoseDB) loadIdxFromFiles() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}
	//WaitGroup等待一组goroutine完成。
	//主goroutine调用Add来设置要等待的goroutine的数量。然后每个goroutine运行并在完成时调用Done。
	//同时，Wait可以用来阻塞直到所有的goroutine都完成。WaitGroup不能在第一次使用后复制
	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for dataType := 0; dataType < DataStructureNum; dataType++ {
		go func(dType uint16) {
			defer func() {
				//Done将WaitGroup计数器减1
				wg.Done()
			}()

			//已归档文件
			var fileIds []int
			dbFile := make(map[uint32]*storage.DBFile)
			for k, v := range db.archFiles[dType] {
				dbFile[k] = v
				fileIds = append(fileIds, int(k))
			}

			//活跃文件
			dbFile[db.activeFileIds[dType]] = db.activeFile[dType]
			fileIds = append(fileIds, int(db.activeFileIds[dType]))

			//按照指定的顺序加载db文件
			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df := dbFile[fid]
				var offset int64 = 0

				for offset <= db.config.BlockSize {
					if e, err := df.Read(offset); err == nil {
						idx := &index.Indexer{
							Meta:      e.Meta,
							FileId:    fid,
							EntrySize: e.Size(),
							Offset:    offset,
						}
						offset += int64(e.Size())

						if len(e.Meta.Key) > 0 {
							if err := db.buildIndex(e, idx); err != nil {
								log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
							}
						}
					} else {
						//表示没有任何的输入
						if err == io.EOF {
							break
						}
						log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
					}
				}
			}
		}(uint16(dataType))
	}
	wg.Wait()
	return nil
}
