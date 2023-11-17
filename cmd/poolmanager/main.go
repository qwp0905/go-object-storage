package main

import (
	"context"
	"flag"
	"time"

	"github.com/qwp0905/go-object-storage/internal/nodepool"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/redis/go-redis/v9"
)

var (
	redisHost string
	redisDb   int
	sec       int
)

func main() {
	flag.StringVar(&redisHost, "redis", "localhost:6379", "redis host")
	flag.IntVar(&redisDb, "db", 1, "redis db")
	flag.IntVar(&sec, "interval", 30, "interval to check health")

	flag.Parse()
	ctx := context.Background()

	rc := redis.NewClient(&redis.Options{Addr: redisHost, DB: redisDb})
	manager := nodepool.NewPoolManager(rc)

	timer := time.NewTicker(time.Second * time.Duration(sec))
	for range timer.C {
		nodes, err := manager.GetAllNodes(ctx)
		if err != nil {
			logger.Errorf("%+v", err)
			continue
		}
		for _, id := range nodes {
			if err := manager.HealthCheck(ctx, id); err == nil {
				continue
			}
			logger.Errorf("%+v", err)
			if err := manager.SetNodeDown(ctx, id); err != nil {
				logger.Errorf("%+v", err)
				continue
			}
		}
	}
}
