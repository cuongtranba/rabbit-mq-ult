package main

import (
	"context"
	"job-queue/queue"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/subosito/gotenv"
)

func init() {
	err := gotenv.Load()
	if err != nil {
		log.Fatal(err)
	}

	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.Info(GetAllEnv())
}

func main() {
	quit := make(chan bool)
	manager := &queue.Manager{Quit: quit}
	ctx, cancelF := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	manager.Run(ctx, cancelF, &wg, queue.NewExampleQueue())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Info("worker is shutting down")
		defer close(sigs)
		cancelF()
		wg.Wait()
		quit <- true
	}()
	<-quit
	wg.Wait()
}

//GetAllEnv export env variables
func GetAllEnv() string {
	var sb strings.Builder
	for _, e := range os.Environ() {
		sb.WriteString(e)
		sb.WriteString("-")
	}
	return sb.String()
}
