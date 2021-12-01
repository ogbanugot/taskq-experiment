package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/vmihailenco/taskq/example/api_worker/sqs_api_worker"
)

func main() {
	flag.Parse()

	//go sqs_api_worker.LogStats()

	go func() {
		for i := 0; i < 2000000000; i++ {
			text := fmt.Sprint(i, "message")
			msg := sqs_api_worker.CountTask.WithArgs(context.Background(), text)
			msg.Name = fmt.Sprint(i, "message")
			err := sqs_api_worker.MainQueue.Add(msg)
			if err != nil {
				log.Fatal(err)
			}
		}
	}()

	sig := sqs_api_worker.WaitSignal()
	log.Println(sig.String())
}
