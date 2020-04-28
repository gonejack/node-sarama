package producer

type Publisher interface {
	Start() (err error)
	Stop() (err error)
	Input() chan interface{}
}
