package main

import (
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"strconv"
)

// docker-compose up kafka
// run app
func main() {

	const addr = "127.0.0.1:9092"
	const topic = "my-topic1"
	const tcp = "tcp"

	// to connect to the kafka leader via an existing non-leader connection rather than using DialLeader
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var connLeader *kafka.Conn
	connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		log.Fatal(err.Error())
	}
	defer connLeader.Close()

}
