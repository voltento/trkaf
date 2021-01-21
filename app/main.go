package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const addr = "127.0.0.1:9092"
const topic = "my-topic"
const tcp = "tcp"

func isWriterApp() bool {
	isWriter := flag.Bool("w", false, "is writer app")
	flag.Parse()
	return *isWriter
}

func main() {
	if isWriterApp() {
		log.Printf("start writing to the topic: %s\n", topic)
		i := 0
		for {
			writeToKafka([]byte(strconv.Itoa(i)))
			<-time.After(time.Second * 2)
			i += 1
		}
	} else {
		log.Printf("start read from the topic: %s\n", topic)
		const flags = log.Lmsgprefix | log.Ldate | log.Lmicroseconds
		readFromKafka(log.New(os.Stdout, "worker_1: ", flags))
	}
}

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
func writeToKafka(msg []byte) {
	partition := 0

	conn, err := kafka.DefaultDialer.DialLeader(context.TODO(), tcp, addr, topic, partition)
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
		log.Printf("Wrote message '%s'\n", string(msg))
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func readFromKafka(logger *log.Logger) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         []string{addr},
		GroupID:         "consumer-group-id",
		Topic:           topic,
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
