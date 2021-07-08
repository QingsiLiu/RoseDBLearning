package cmd

import (
	"fmt"
	"github.com/roseduan/rosedb"
	"github.com/tidwall/redcon"
	"log"
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

//实例化服务
func NewServer(config rosedb.Config) (*Server, error) {
	db, err := rosedb.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db}, nil
}

//监听服务
func (s *Server) Listen(addr string) {
	//NewServerNetwork返回一个新的Redcon服务器。网络网络必须是面向流的网络:"tcp"， "tcp4"， "tcp6"， "unix"或"unixpacket"
	svr := redcon.NewServerNetwork("tcp", addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			s.handleCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
		})

	s.server = svr
	log.Println("rosedb is running, ready to accept connections.")
	//为传入的连接提供服务，并在监听时传递nil或错误。信号可以为零
	if err := svr.ListenAndServe(); err != nil {
		log.Printf("listen and serve ocuurs error: %+v", err)
	}
}

func (s *Server) handleCmd(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	exec, exist := ExecCmd[command]
	if !exist {
		conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", command))
		return
	}
	args := make([]string, 0, len(cmd.Args)-1)
	for i, bytes := range cmd.Args {
		if i == 0 {
			continue
		}
		args = append(args, string(bytes))
	}
	//服务器执行结果获得返回值
	reply, err := exec(s.db, args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteAny(reply)
}
