package kafka

import (
	"errors"
	"fmt"
	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	Hosts         []string             // Kafka主机
	Ptopic        string               // Topic
	SendMsg       string               // 发送的消息
	AsyncProducer sarama.AsyncProducer // Kafka生产者接口对象
}

func (k *KafkaProducer) kafkaInit() {
	// 定义配置参数
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Version = sarama.V0_10_2_0

	// 初始化一个生产者对象
	producer, err := sarama.NewAsyncProducer(k.Hosts, config)

	if err != nil {
		err = errors.New("NewAsyncProducer错误,原因:" + err.Error())
		fmt.Println(err.Error())
		return
	}

	// 保存对象到结构体
	k.AsyncProducer = producer
}

func (k *KafkaProducer) kafkaProcess() {
	msg := &sarama.ProducerMessage{
		Topic: k.Ptopic,
	}
	// 信息编码
	msg.Value = sarama.ByteEncoder(k.SendMsg)
	// 将信息发送给通道
	k.AsyncProducer.Input() <- msg
}
