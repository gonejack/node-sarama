package wrapper

import (
	"github.com/bsm/sarama-cluster"
	"github.com/spf13/viper"
	"strings"
	"sync"
)

type ConsumerWrapper struct {
	clusterConfig   *viper.Viper
	topicConsumers  sync.Map
	topicSubsQueues sync.Map
}

func (cw *ConsumerWrapper) Subscribe(groupId string, topic string) (queue chan []byte) {
	queue = make(chan []byte, 300)

	if topicSubsQueues, exist := cw.topicSubsQueues.Load(topic); exist {
		cw.topicSubsQueues.Store(topic, append(topicSubsQueues.([]chan []byte), queue))
	} else {
		logger.Infof("构建消费者[topic=%s]", topic)
		config := cluster.NewConfig()
		config.Consumer.Return.Errors = true
		config.Net.SASL.Enable = cw.clusterConfig.GetBool("sasl.enable")
		config.Net.SASL.User = cw.clusterConfig.GetString("sasl.user")
		config.Net.SASL.Password = cw.clusterConfig.GetString("sasl.password")
		brokers := strings.Split(cw.clusterConfig.GetString("brokers"), ",")
		consumer, err := cluster.NewConsumer(brokers, groupId, []string{topic}, config)

		if err == nil {
			cw.topicConsumers.Store(topic, consumer)
			cw.topicSubsQueues.Store(topic, []chan []byte{queue})

			go cw.readThread(consumer, topic)

			logger.Infof("构建消费者[topic=%s]完成", topic)
		} else {
			logger.Errorf("构建消费者出错[topic=%s]出错: %s", topic, err)
		}
	}

	return
}

func (cw *ConsumerWrapper) Unsubscribe(unSubscribeQueue chan []byte) (ok bool) {
	cw.topicSubsQueues.Range(func(k, v interface{}) bool {
		topic, subQueues := k.(string), v.([]chan []byte)

		for index, subQueue := range subQueues {
			if subQueue == unSubscribeQueue {
				ok = true

				logger.Infof("消费者[topic=%s]减少一个订阅", topic)
				subQueues = append(subQueues[0:index], subQueues[index+1:]...)

				if len(subQueues) == 0 {
					logger.Infof("消费者[topic=%s]没有订阅，执行关闭", topic)

					value, exist := cw.topicConsumers.Load(topic)

					if exist {
						consumer := value.(*cluster.Consumer)

						if err := consumer.Close(); err == nil {
							logger.Infof("消费者[topic=%s]已关闭", topic)
						} else {
							logger.Errorf("关闭消费者[topic=%s]出错: %s", topic, err)
						}
					}

					cw.topicConsumers.Delete(topic)
					cw.topicSubsQueues.Delete(topic)
				} else {
					cw.topicSubsQueues.Store(topic, subQueues)
				}

				return false
			}
		}

		return true
	})

	return
}

func (cw *ConsumerWrapper) readThread(consumer *cluster.Consumer, topic string) {
	for {
		select {
		case message, ok := <-consumer.Messages():
			if ok {
				subQueues, _ := cw.topicSubsQueues.Load(topic)

				for _, subQueue := range subQueues.([]chan []byte) {
					subQueue <- message.Value
				}
			} else {
				logger.Infof("消费者[topic=%s]退出读取线程", topic)

				return
			}
		case err := <-consumer.Errors():
			if err != nil {
				logger.Errorf("消费者[topic=%s]出错：%s", topic, err)
			}
		}
	}
}

func (cw *ConsumerWrapper) Close() {
	cw.topicConsumers.Range(func(k, v interface{}) bool {
		topic, consumer := k.(string), v.(*cluster.Consumer)

		if err := consumer.Close(); err == nil {
			logger.Debugf("消费者[topic=%s]已关闭", topic)
		} else {
			logger.Errorf("关闭消费者[topic=%s]出错: %s", topic, err)
		}

		return true
	})
}

func NewConsumerWrapper(conf *viper.Viper) (cw *ConsumerWrapper) {
	cw = &ConsumerWrapper{
		clusterConfig: conf,
	}

	return
}
