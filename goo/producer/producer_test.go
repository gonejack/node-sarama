package producer

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
	WaitForAll: true,
	Version:    "0.10.0.1",
}

var pub Publisher

func TestMain(m *testing.M) {
	pub = New(config)

	m.Run()
}

func TestPublisher(t *testing.T) {
	err := pub.Start()
	if err != nil {
		t.Fatalf("启动失败: %s", err)
	}

	go func() {
		for i := 0; i < 10; i++ {
			fmt.Printf("发送消息%d\n", i)
			pub.Input() <- NewMessage("test_topic", []byte(fmt.Sprintf("消息%d", i)))
		}
	}()

	time.Sleep(time.Second * 5)

	err = pub.Stop()
	if err != nil {
		t.Fatalf("关闭失败: %s", err)
	}
}
