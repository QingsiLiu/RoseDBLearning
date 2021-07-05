package cmd

import (
	"github.com/roseduan/rosedb"
	"github.com/tidwall/redcon"
	"strings"
	"sync"
)

//CMD执行命令的Func
type ExecCmdFunc func(*rosedb.RoseDB, []string) (interface{}, error)

//执行CMD的映射函数，保存与指定命令对应的所有函数
var ExecCmd = make(map[string]ExecCmdFunc)

//每个rosedb数据库的服务结构体
type Server struct {
	server *redcon.Server
	db     *rosedb.RoseDB
	closed bool
	mu     sync.Mutex
}

//将命令对应的执行函数添加到表中
func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

func NewServer(config rosedb.Config) (*Server, error) {
	db, err := rosedb.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db}, nil
}
