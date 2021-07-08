/*
针对数据库中的字符串操作命令
*/
package cmd

import (
	"fmt"
	"github.com/roseduan/rosedb"
	"github.com/tidwall/redcon"
)

//SimpleString用于表示来自*Any调用的字符串的非bulk表示
var okResult = redcon.SimpleString("OK")

func init() {
	addExecCommand("set", set)
	addExecCommand("get", get)
}

//字符串操作指令报错
func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("wrong number of arguments for '%S' command", cmd)
}

//针对字符串类型的set命令
func set(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("set")
		return
	}

	key, value := args[0], args[1]
	if err = db.Set([]byte(key), []byte(value)); err == nil {
		res = okResult
	}
	return
}

//针对字符串类型的get命令
func get(db *rosedb.RoseDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("set")
		return
	}

	key := args[0]
	var val []byte
	if val, err = db.Get([]byte(key)); err == nil {
		res = string(val)
	}
	return
}
