package producer

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type publisher struct {
	Config
	saramaC  *sarama.Config
	producer sarama.AsyncProducer
	input    chan interface{}

	sync.WaitGroup
}

func (p *publisher) String() string {
	return fmt.Sprintf("生产者(name=%s, brokers=%v)", p.Name, p.Brokers)
}

func (p *publisher) Start() (err error) {
	p.producer, err = sarama.NewAsyncProducer(p.Brokers, p.saramaC)

	if err == nil {
		go func() {
			p.Add(1)
			Logger.Printf("%s启动主线程", p)
			p.mainRoutine()
			Logger.Printf("%s关闭主线程", p)
			p.Done()
		}()
	}

	return
}

func (p *publisher) mainRoutine() {
	for {
		select {
		case data, ok := <-p.input:
			if ok {
				message := data.(*sarama.ProducerMessage) //  这里会出错一定是用法不对，直接panic就可以了

				p.producer.Input() <- message

				if p.Debug {
					Logger.Printf("%s发送消息(topic=%s, message=%s) ", p, message.Topic, message.Value)
				}
			} else {
				time.Sleep(time.Second / 10)
			}
		case msg, ok := <-p.producer.Successes():
			if ok {
				if p.Debug {
					Logger.Printf("%s成功发送消息: %s", p, msg)
				}
			} else {
				return
			}
		case err, ok := <-p.producer.Errors():
			if ok {
				Logger.Printf("%s出错: %s", p, err)
			} else {
				return
			}
		}
	}
}

func (p *publisher) Stop() (err error) {
	close(p.input)

	err = p.producer.Close()

	p.Wait()

	return
}

func (p *publisher) Input() chan interface{} {
	return p.input
}

func New(conf Config) (p Publisher) {
	sconf := sarama.NewConfig()

	if conf.WaitForAll {
		sconf.Producer.RequiredAcks = sarama.WaitForAll
	}
	if conf.ReturnSuc {
		sconf.Producer.Return.Successes = true
	}
	if conf.SASL.Enable {
		sconf.Net.SASL.Enable = true
		sconf.Net.SASL.User = conf.SASL.User
		sconf.Net.SASL.Password = conf.SASL.Password
	}
	if conf.Version != "" {
		kafkaVersion, err := sarama.ParseKafkaVersion(conf.Version)
		if err == nil {
			sconf.Version = kafkaVersion
			Logger.Printf("Kafka 版本配置为: %s", kafkaVersion)
		} else {
			Logger.Fatalf("错误的 Kafka 版本配置: %s", conf.Version)
		}
	}

	p = &publisher{
		Config:  conf,
		saramaC: sconf,
		input:   make(chan interface{}, 10),
	}

	return
}

func NewMessage(topic string, data []byte) (msg *sarama.ProducerMessage) {
	msg = &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	return
}
