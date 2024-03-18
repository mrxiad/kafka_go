package kafka

// Kafka方法接口
type IKafkaMethod interface {
	kafkaInit()    // 初始化方法
	kafkaProcess() // 执行方法
}

// KafkaManager 接口管理结构体
type KafkaManager struct {
	kafkaMethod IKafkaMethod // 接口对象
}

// Run 定义实现Run方法
func (km *KafkaManager) Run() {
	km.kafkaMethod.kafkaInit()
	go km.kafkaMethod.kafkaProcess()
}

// Set 定义实现Set方法
func (km *KafkaManager) Set(m IKafkaMethod) {
	km.kafkaMethod = m // 将指定的方法赋给接口
}

// KafkaMessager
type KafkaMessager struct {
	KafkaManager  *KafkaManager  // 接口管理对象指针
	KafkaProducer *KafkaProducer // 生产者对象指针
	KafkaConsumer *KafkaConsumer // 消费者对象指针
	Hosts         []string       // Kafka主机
	topic         string         // topic
}

// NewKafkaMessager 供外部调用初始化的函数,传入Kafka主机IP和Topic,返回操作对象指针,并初始化结构体成员变量
func NewKafkaMessager(hosts []string, topic string) *KafkaMessager {
	km := &KafkaMessager{
		KafkaManager:  new(KafkaManager),
		KafkaProducer: new(KafkaProducer),
		KafkaConsumer: new(KafkaConsumer),
		Hosts:         hosts,
		topic:         topic,
	}
	return km
}
