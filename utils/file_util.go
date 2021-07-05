package utils

import "os"

//检查路径目录或文件是否存在
func Exist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}
