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
	//测试
	myKafka := kafka.NewKafkaMessager(hosts, topic)

	fmt.Println("开启消费者")
	// 初始化消费者
	myKafka.KafkaConsumer.Hosts = hosts             // 消费者host赋值
	myKafka.KafkaConsumer.Ctopic = topic            // 消费者topic赋值
	myKafka.KafkaConsumer.Kchan = make(chan string) // 初始化消息通道
	myKafka.KafkaManager.Set(myKafka.KafkaConsumer) // 将消费者对象赋给接口
	myKafka.KafkaManager.Run()                      // 启动消费者


	fmt.Println("开启生产者")
	//初始化生产者
	myKafka.KafkaProducer.Hosts = hosts  // 生产者host赋值``
	myKafka.KafkaProducer.Ptopic = topic // 生产者topic赋值
	myKafka.KafkaProducer.SendMsg = "shabi"
	myKafka.KafkaManager.Set(myKafka.KafkaProducer) // 将生产者对象赋给接口
	myKafka.KafkaManager.Run()                      // 启动生产者
	fmt.Println("生产完毕")

	// 监听通道,接收生产客户端发过来的消息
	recv := <-myKafka.KafkaConsumer.Kchan
	fmt.Println(recv) // 打印接收到的消息
}

