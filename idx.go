package RoseDB

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
