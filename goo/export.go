package main

import (
	"fmt"
	"github.com/Shopify/sysv_mq"
	"log"
	"time"
)

func main() {
	mq, err := sysv_mq.NewMessageQueue(&sysv_mq.QueueConfig{
		Key:     0x0000303d,               // SysV IPC key
		MaxSize: 1024,                     // Max size of a message
		Mode:    sysv_mq.IPC_CREAT | 0666, // Creates if it doesn't exist, 0600 permissions
	})
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Destroy()

	for {
		data, mtype, err := mq.ReceiveString(1, 0)
		if err == nil {
			fmt.Printf("[%d] %s", mtype, data)
		} else {
			break
		}

		time.Sleep(time.Second)
	}
}
