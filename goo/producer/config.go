package producer

import (
	"github.com/gonejack/node-sarama/goo/logger"
	"io/ioutil"
	"log"
)

var Logger logger.Logger = log.New(ioutil.Discard, "[Producer] ", log.LstdFlags)

type Config struct {
	Name    string
	Brokers []string // 服务器列表
	SASL    struct {
		Enable   bool // 是否启用加密信息，没有密码的服务器不要设置为true，即使不填user和pass也连不上
		User     string
		Password string
	}

	ReturnSuc  bool   // 打印发送成功的信息
	WaitForAll bool   // 所有kafka节点都要收到消息才继续
	Version    string // 驱动应该用什么版本的api与服务器对话，最好和服务器版本保持一致以避免怪问题
	Debug      bool
}
