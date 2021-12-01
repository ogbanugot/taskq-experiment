package sqs_api_worker

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/redisq"
)

func NewClient() (*redis.Client, taskq.Factory) {
	//dsn := "stupefied_bouman://localhost:8379"

	//opts, err := redis.ParseURL(dsn)
	//if err != nil {
	//	return nil, nil, err
	//}

	Redis := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	//if err := Redis.
	//	Ping(context.Background()).
	//	Err(); err != nil {
	//	fmt.Print(err)
	//	return nil, nil, err
	//}

	QueueFactory := redisq.NewFactory()
	return Redis, QueueFactory
}

var (
	Redis, QueueFactory = NewClient()
	MainQueue           = QueueFactory.RegisterQueue(&taskq.QueueOptions{
		Name:  "api-worker3",
		Redis: Redis,
	})
)
var C = MainQueue.Consumer()

var CountTask = taskq.RegisterTask(&taskq.TaskOptions{
	Name: "printer",
	Handler: func(name string) error {
		fmt.Println("Hello", name)
		return nil
	},
})

var counter int32

func GetLocalCounter() int32 {
	return atomic.LoadInt32(&counter)
}

func IncrLocalCounter() {
	atomic.AddInt32(&counter, 1)
}

func LogStats() {
	var prev int32
	for range time.Tick(3 * time.Second) {
		n := GetLocalCounter()
		log.Printf("processed %d tasks (%d/s)", n, (n-prev)/3)
		prev = n
	}
}

func WaitSignal() os.Signal {
	ch := make(chan os.Signal, 2)
	signal.Notify(
		ch,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGTERM,
	)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM:
			return sig
		}
	}
}
