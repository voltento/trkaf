package kf

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"strconv"
	"time"
)

type Kafka struct {
	Scheme string
	Addr   string
}

func NewKafka(scheme string, addr string) *Kafka {
	return &Kafka{
		Scheme: scheme,
		Addr:   addr,
	}
}

func (k *Kafka) ShowTopics() {
	conn, err := kafka.Dial(k.Scheme, k.Addr)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	for k := range m {
		fmt.Println(k)
	}
}

// The method will create topic if it doesn't exist if kafka option AUTO_CREATE_TOPICS_ENABLE=yes is set
func (k *Kafka) WriteToKafka(topicName string, partition int, msg []byte) {
	conn, err := kafka.DefaultDialer.DialLeader(context.TODO(), k.Scheme, k.Addr, topicName, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	// conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	defer conn.Close()

	_, err = conn.WriteMessages(
		kafka.Message{Value: msg},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	} else {
		log.Printf("Wrote to partition '%d' message '%s'\n", partition, string(msg))
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func (k *Kafka) ReadFromKafka(topicName string, logger *log.Logger) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         []string{k.Addr},
		GroupID:         "1",
		Topic:           topicName,
		ReadLagInterval: time.Second,
		MaxWait:         time.Second,
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		logger.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func (k *Kafka) CreateTopic(topic kafka.TopicConfig) {
	conn, err := kafka.Dial(k.Scheme, k.Addr)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(topic)
	if err != nil {
		log.Fatal(err.Error())
	}
}
