package main

import (
	"flag"
	"fmt"
	"github.com/pelletier/go-toml"
	"github.com/roseduan/rosedb"
	"github.com/roseduan/rosedb/cmd"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	banner, _ := ioutil.ReadFile("../../resource/banner.txt")
	fmt.Println(string(banner))
}

// 参数config表示rosedb的配置文件路径
// 默认配置文件请参见config.toml
var config = flag.String("config", "D:/GoWorkSpace/src/RoseDB/config.toml", "the config file for rosedb")

// 参数dirPath表示db文件和其他配置文件的持久目录
var dirPath = flag.String("dir_path", "D:/GoWorkSpace/src/RoseDB/data", "the dir path for the database")

func main() {
	//解析命令行参数写入注册的flag里
	flag.Parse()

	//设置配置
	var cfg rosedb.Config
	if *config == "" {
		log.Println("no config set, using the default config.")
	} else {
		//通过配置文件创建一个配置结构体（结构体以指针类型进行值传递）
		c, err := newConfigFromFile(*config)
		if err != nil {
			log.Printf("load config err : %+v\n", err)
			return
		}
		cfg = *c
	}

	if *dirPath == "" {
		log.Println("no dir path set, using the os tmp dir.")
	} else {
		cfg.DirPath = *dirPath
	}

	//监听服务(操作系统的信号机制)
	sig := make(chan os.Signal, 1)
	//通知使包信号将传入信号中继到sig中
	//中断、强制退出、终端挂起或者控制进程终止、键盘中断、终止信号、键盘的退出键被按下
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	server, err := cmd.NewServer(cfg)
	if err != nil {
		log.Printf("create rosedb server err: %+v\n", err)
		return
	}
	go server.Listen(cfg.Addr)

	<-sig
	server.Stop()
	log.Println("rosedb is ready to exit, bye...")
}

//从文件从读取新的配置文件
func newConfigFromFile(config string) (*rosedb.Config, error) {
	data, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, err
	}

	var cfg = new(rosedb.Config)
	//Unmarshal解析toml编码的数据并将结果存储在值中
	err = toml.Unmarshal(data, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
