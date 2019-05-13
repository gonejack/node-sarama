package wrapper

import (
	"github.com/Shopify/sarama"
	"github.com/gonejack/glogger"
	"github.com/spf13/viper"
	"strings"
)

var logger = glogger.NewLogger("producer")

type ProduceWrapperType struct {
	clusterConfig *viper.Viper
	producer      sarama.AsyncProducer
}

func (pw *ProduceWrapperType) GetInputQueue(topic string) (queue chan []byte) {
	queue = make(chan []byte, 300)

	go pw.writeMessageThread(topic, queue)

	return
}

func (pw *ProduceWrapperType) writeMessageThread(topic string, msgQueue chan []byte) {
	for msg := range msgQueue {
		pw.producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(msg),
		}
	}
}

func (pw *ProduceWrapperType) readErrorThread() {
	for err := range pw.producer.Errors() {
		logger.Errorf("生产者[cluster=%s]出错: %s", pw.clusterConfig.GetString("name"), err)
	}

	logger.Infof("生产者停止")
}

func (pw *ProduceWrapperType) Close() {
	if err := pw.producer.Close(); err == nil {
		logger.Infof("生产者[cluster=%s]已关闭", pw.clusterConfig.GetString("name"))
	} else {
		logger.Errorf("关闭生产者[cluster=%s]出错: %s", pw.clusterConfig.GetString("name"), err)
	}
}

func NewProducerWrapper(conf *viper.Viper) (pw *ProduceWrapperType) {
	pw = &ProduceWrapperType{
		clusterConfig: conf,
	}

	if pw.producer == nil {
		logger.Infof("构建生产者[cluster=%s]", conf.GetString("name"))

		config := sarama.NewConfig()
		config.Producer.Retry.Max = 1
		config.Producer.RequiredAcks = sarama.WaitForAll
		//config.Producer.Return.Successes = true
		config.Net.SASL.Enable = pw.clusterConfig.GetBool("sasl.enable")
		config.Net.SASL.User = pw.clusterConfig.GetString("sasl.user")
		config.Net.SASL.Password = pw.clusterConfig.GetString("sasl.password")
		brokers := strings.Split(pw.clusterConfig.GetString("brokers"), ",")
		producer, err := sarama.NewAsyncProducer(brokers, config)

		if err == nil {
			pw.producer = producer

			go pw.readErrorThread()

			logger.Infof("构建生产者[cluster=%s]完成", conf.GetString("name"))
		} else {
			logger.Errorf("构建生产者[cluster=%s]出错: %s", conf.GetString("name"), err)
		}
	}

	return
}
