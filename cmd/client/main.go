package main

import (
	"flag"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/peterh/liner"
	"log"
	"os"
	"strings"
)

//支持的指令列表
var commandList = [][]string{
	{"SET", "key value", "STRING"},
	{"GET", "key", "STRING"},
}

//默认的host以及port(flag包实现命令行参数的解析),此时的host以及port都是指针类型
var host = flag.String("h", "127.0.0.1", "the rosedb server host, default 127.0.0.1")
var port = flag.Int("p", 5200, "the port server port, default 5200")

//默认的cmd地址
const cmdHistoryPath = "D:/GoWorkSpace/src/RoseDB/cmd/client"

func main() {
	//解析命令行参数写入注册的flag里
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Println("tcp dial err: ", err)
		return
	}

	//命令行实例化
	line := liner.NewLiner()
	defer line.Close()

	//ctrl+C 可以退出返回
	line.SetCtrlCAborts(true)
	//接受光标左侧当前编辑的行内容，并返回一个补全候选列表
	//SetCompleter设置了当用户按tab键时，Liner将调用的补全函数来获取补全候选项
	line.SetCompleter(func(li string) (res []string) {
		for _, c := range commandList {
			if strings.HasPrefix(c[0], strings.ToUpper(li)) {
				res = append(res, strings.ToLower(c[0]))
			}
		}
		return
	})

	//保存命令行的历史记录
	if f, err := os.Open(cmdHistoryPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}
	defer func() {
		//如果没有文件则创建文件，有文件则打开
		if f, err := os.Create(cmdHistoryPath); err == nil {
			line.WriteHistory(f)
			f.Close()
		} else {
			fmt.Printf("cmd histroy err: %v\n", err)
		}
	}()

	//先将命令关键字提取出来并全部转换为小写
	commandSet := map[string]bool{}
	for _, cmd := range commandList {
		commandSet[strings.ToLower(cmd[0])] = true
	}

	prompt := addr + ">"
	for {
		//显示并返回用户输入的一行，不包括末尾换行符
		cmd, err := line.Prompt(prompt)
		if err != nil {
			fmt.Println(err)
			break
		}

		//去掉首尾的空格
		cmd = strings.TrimSpace(cmd)
		if len(cmd) == 0 {
			continue
		}
		lowerCmd := strings.ToLower(cmd)

		//将用户输入的字符串分割开
		c := strings.Split(cmd, " ")

		if lowerCmd == "quit" {
			break
		} else { // 执行命令并返回服务器结果
			// 将执行的命令添加到回滚历史中
			line.AppendHistory(cmd)

			lowerC := strings.ToLower(strings.TrimSpace(c[0]))
			if !commandSet[lowerC] && lowerC != "quit" {
				continue
			}

			command, args := parseCommandLine(cmd)
			//向服务器发送命令并返回接收到的回复
			rawResp, err := conn.Do(command, args...)
			if err != nil {
				fmt.Printf("(error) %v \n", err)
				continue
			}

			switch reply := rawResp.(type) {
			case []byte:
				println(string(reply))
			case string:
				println(reply)
			case nil:
				println("nil")
			case redis.Error:
				fmt.Printf("(error) %v \n", reply)
			case int64:
				fmt.Printf("(integer) %d \n", reply)
			case []interface{}:
				for i, e := range reply {
					switch element := e.(type) {
					case string:
						fmt.Printf("%d) %s\n", i+1, element)
					case []byte:
						fmt.Printf("%d) %s\n", i+1, string(element))
					default:
						fmt.Printf("%d) %v\n", i+1, element)
					}
				}
			}
		}
	}
}

//解析命令行，返回命令以及参数
func parseCommandLine(cmdLine string) (string, []interface{}) {
	arr := strings.Split(cmdLine, " ")
	if len(arr) == 0 {
		return "", nil
	}
	args := make([]interface{}, 0)
	for i := 1; i < len(arr); i++ {
		args = append(args, arr[i])
	}
	return arr[0], args
}
