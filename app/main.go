package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	go readFromKafka()
	showTopics()
	writeToKafka()
	<-time.After(time.Second * 10)
}

const addr = "127.0.0.1:9092"
const topic = "my-topic1"
const tcp = "tcp"

func showTopics() {
	conn, err := kafka.Dial(tcp, addr)
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
func writeToKafka() {
	partition := 0

	if _, err := kafka.Dial(tcp, addr); err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn, err := kafka.DefaultDialer.DialLeader(context.TODO(), tcp, addr, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	defer conn.Close()

	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func readFromKafka() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         []string{addr},
		GroupID:         "consumer-group-id",
		Topic:           topic,
		ReadLagInterval: time.Second,
		MaxWait:         time.Second,
	})

	r.SetOffset(0)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
