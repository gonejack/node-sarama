package consumer

import (
	"C"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// sarama-cluster 实现的订阅器
type subscriber struct {
	Config

	saramaConf *cluster.Config
	consumer   *cluster.Consumer
	output     chan interface{}

	sync.WaitGroup
}

// 启动方法
func (s *subscriber) Start() (err error) {
	if s.Debug {
		Logger.Printf("构建%s", s)
	}

	s.consumer, err = cluster.NewConsumer(s.Brokers, s.GroupId, s.Topics, s.saramaConf)
	if err == nil {
		if s.Debug {
			Logger.Printf("构建%s完成", s)
		}

		go func() {
			if s.Debug {
				Logger.Printf("%s开启读取线程", s)
			}
			s.Add(1)
			s.mainRoutine()
			s.Done()
			if s.Debug {
				Logger.Printf("%s退出读取线程", s)
			}
		}()
	} else {
		err = fmt.Errorf("构建%s出错: %s", s, err)
	}

	return
}

// 关闭方法
func (s *subscriber) Stop() (err error) {
	err = s.consumer.Close()

	s.Wait()

	if err == nil {
		if s.Debug {
			Logger.Printf("%s已关闭", s)
		}
	} else {
		err = fmt.Errorf("%s关闭出错: %s", s, err)
	}

	return
}

// 获取消息输出队列
func (s *subscriber) Output() (queue chan interface{}) {
	return s.output
}

// 消息读取线程
func (s *subscriber) mainRoutine() {
	for {
		select {
		case msg, ok := <-s.consumer.Messages():
			if ok {
				s.output <- Message{Topic: msg.Topic, Key: msg.Key, Value: msg.Value}

				s.consumer.MarkOffset(msg, "")
			} else {
				close(s.output) // 重要
				return
			}
		case err, ok := <-s.consumer.Errors():
			if ok && err != nil {
				Logger.Printf("%s出错: %s", s, err)
			}
		case n, ok := <-s.consumer.Notifications():
			if ok {
				Logger.Printf("%s发出通知: %s", s, n.Type)
				Logger.Printf("减持分区 %+v", n.Released)
				Logger.Printf("增持分区 %+v", n.Claimed)
				Logger.Printf("当前持有 %+v", n.Current)
			}
		}
	}
}

// 自定义打印
func (s *subscriber) String() string {
	return fmt.Sprintf("消费者(name=%s, groupId=%s, topics=%v)", s.Name, s.GroupId, s.Topics)
}

// 构建方法
func New(conf Config) (ks Subscriber) {
	sconf := cluster.NewConfig()
	sconf.Consumer.Return.Errors = true
	sconf.Group.Return.Notifications = true

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

	ks = &subscriber{
		Config:     conf,
		saramaConf: sconf,
		output:     make(chan interface{}, 100),
	}

	return
}
