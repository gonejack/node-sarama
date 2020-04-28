package consumer

// 订阅器接口定义
type Subscriber interface {
	Start() (err error)
	Stop() (err error)
	Output() chan interface{}
}
