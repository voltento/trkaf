package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	showTopics()
	writeToKafka()
	readFromKafka()
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
	// to produce messages
	partition := 0

	if _, err := kafka.Dial(tcp, addr); err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	p, err := kafka.DefaultDialer.LookupPartition(context.TODO(), tcp, addr, topic, partition)
	if err != nil {
		log.Fatal("can not lookup partition: " + err.Error())
	}
	p.Leader.Host = "localhost"
	conn, err := kafka.DefaultDialer.DialPartition(context.TODO(), tcp, addr, p)

	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
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
		Brokers:   []string{addr},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("read error: %s", err.Error())
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
