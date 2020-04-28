package consumer

import (
	"fmt"
	"testing"
	"time"
)

var config = Config{
	Brokers: []string{
		"192.168.11.30:9093",
		"192.168.11.31:9093",
		"192.168.11.32:9093",
	},
	SASL: struct {
		Enable   bool
		User     string
		Password string
	}{
		Enable:   false,
		User:     "",
		Password: "",
	},
	GroupId: "test_group",
	Topics:  []string{"test_topic"},
	Version: "0.10.0.1",
}

var sub Subscriber

func TestMain(m *testing.M) {
	sub = New(config)

	m.Run()
}

func TestKafkaSubscriber(t *testing.T) {
	err := sub.Start()
	if err != nil {
		t.Fatalf("启动失败: %s", err)
	}

	go func() {
		for data := range sub.Output() {
			fmt.Printf("读取到消息: %s\n", data.(Message))
		}
	}()

	time.Sleep(time.Second * 30)

	err = sub.Stop()
	if err != nil {
		t.Fatalf("关闭失败: %s", err)
	}
}
