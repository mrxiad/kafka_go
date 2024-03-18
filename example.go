package main

import (
	"fmt"
	"kafka_go/kafka"
)

var (
	hosts = []string{"localhost:9092"}
	topic = "test"
)

func main() {
	//测试消费者
	myKafka := kafka.NewKafkaMessager(hosts, topic)

	//初始化消费者
	myKafka.KafkaConsumer.Hosts = hosts             // 消费者host赋值
	myKafka.KafkaConsumer.Ctopic = topic            // 消费者topic赋值
	myKafka.KafkaConsumer.Kchan = make(chan string) // 初始化消息通道
	myKafka.KafkaManager.Set(myKafka.KafkaConsumer) // 将消费者对象赋给接口
	myKafka.KafkaManager.Run()                      // 启动消费者

	// 监听通道,接收生产客户端发过来的消息
	recv := <-myKafka.KafkaConsumer.Kchan
	fmt.Println(recv) // 打印接收到的消息
}
