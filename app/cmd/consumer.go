/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/voltento/trkaf/internal/kf"
)

// consumerCmd represents the consumer command
var consumerCmd = &cobra.Command{
	Use:   "consumer",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("consumer called")
	},
}

func init() {
	rootCmd.AddCommand(consumerCmd)

	const tcp = "tcp"

	const addr = "127.0.0.1:9092"
	const topic = "count_2_partitions"
	conn := kf.NewKafka(tcp, addr)

	log.Printf("start read from the topic: %s\n", topic)
	const flags = log.Lmsgprefix | log.Ldate | log.Lmicroseconds
	conn.ReadFromKafka(topic, log.New(os.Stdout, "worker_1: ", flags))
}
