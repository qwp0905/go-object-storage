package main

import (
	"flag"

	"github.com/qwp0905/go-object-storage/api"
	"github.com/qwp0905/go-object-storage/internal/http"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
	"github.com/redis/go-redis/v9"
)

var (
	addr      uint
	redisHost string
	redisDb   int
	sec       int
)

func main() {
	flag.StringVar(&redisHost, "redis", "localhost:6379", "redis host")
	flag.IntVar(&redisDb, "db", 1, "redis db")
	flag.IntVar(&sec, "interval", 30, "interval to check health")
	flag.UintVar(&addr, "addr", 8080, "listen addr")

	flag.Parse()

	rc := redis.NewClient(&redis.Options{Addr: redisHost, DB: redisDb})
	manager := nodepool.NewPoolManager(rc)
	go manager.Start(sec)

	healthController := api.NewHealth()
	metricsController := api.NewMetrics()

	app := http.NewApplication()
	app.Mount(healthController, metricsController)

	if err := app.Listen(addr); err != nil {
		panic(err)
	}
}
