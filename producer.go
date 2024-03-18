package kafka

import (
	"errors"
	"fmt"
	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	hosts         []string             // Kafka主机
	sendmsg       string               // 消费方返回给生产方的消息
	ptopic        string               // Topic
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
	producer, err := sarama.NewAsyncProducer(k.hosts, config)
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
		Topic: k.ptopic,
	}
	// 信息编码
	msg.Value = sarama.ByteEncoder(k.sendmsg)

	// 将信息发送给通道
	k.AsyncProducer.Input() <- msg
}
