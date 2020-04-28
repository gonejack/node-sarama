package consumer

import (
	"log"

	"io/ioutil"

	"github.com/gonejack/node-sarama/goo/logger"
)

var Logger logger.Logger = log.New(ioutil.Discard, "[Consumer] ", log.LstdFlags)

// kafka 类型订阅点附加配置
type Config struct {
	Name    string
	Brokers []string // 服务器列表
	SASL    struct {
		Enable   bool // 是否启用加密信息，没有密码的服务器不要设置为true，即使不填user和pass也连不上
		User     string
		Password string
	}
	GroupId string   // 消费组
	Topics  []string // topic
	Version string   // 驱动应该用什么版本的api与服务器对话，最好和服务器版本保持一致以避免怪问题
	Debug   bool
}
