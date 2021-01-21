package main

import (
	"flag"
	"github.com/voltento/trkaf/internal/kf"
	"log"
	"os"
	"strconv"
	"time"
)

const addr = "127.0.0.1:9092"
const topic = "count_2_partitions"

func isWriterApp() bool {
	isWriter := flag.Bool("w", false, "is writer app")
	flag.Parse()
	return *isWriter
}

func getRoundIterator(from, to int) func() int {
	if from > to {
		from, to = to, from
	}

	counter := 0
	return func() int {
		r := counter
		counter += 1
		if counter >= to {
			counter = from
		}
		return r
	}
}

func main() {
	const tcp = "tcp"
	conn := kf.NewKafka(tcp, addr)

	if isWriterApp() {
		log.Printf("start writing to the topic: %s\n", topic)
		i := 0
		partition := getRoundIterator(0, 2)
		for {
			conn.WriteToKafka(topic, partition(), []byte(strconv.Itoa(i)))
			<-time.After(time.Second * 2)
			i += 1
		}
	} else {
		log.Printf("start read from the topic: %s\n", topic)
		const flags = log.Lmsgprefix | log.Ldate | log.Lmicroseconds
		conn.ReadFromKafka(topic, log.New(os.Stdout, "worker_1: ", flags))
	}
}
