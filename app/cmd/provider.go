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
	"github.com/voltento/trkaf/internal/kf"
	"log"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

// providerCmd represents the provider command
var providerCmd = &cobra.Command{
	Use:   "provider",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		const tcp = "tcp"
		conn := kf.NewKafka(tcp, addr)
		log.Printf("start writing to the topic: %s\n", topic)
		i := 0
		partition := getRoundIterator(0, 2)
		for {
			conn.WriteToKafka(topic, partition(), []byte(strconv.Itoa(i)))
			<-time.After(time.Second * 2)
			i += 1
		}
	},
}

func init() {
	rootCmd.AddCommand(providerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// providerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// providerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
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
