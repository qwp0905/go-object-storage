package main

import (
	"flag"

	"github.com/qwp0905/go-object-storage/api"
	"github.com/qwp0905/go-object-storage/internal/http"
	"github.com/qwp0905/go-object-storage/internal/namenode"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/redis/go-redis/v9"
)

var app http.Application

var (
	addr      uint
	redisHost string
	redisDb   int
	logLevel  string
)

func main() {
	flag.UintVar(&addr, "addr", 8080, "application addr")
	flag.StringVar(&redisHost, "redis", "localhost:6379", "redis host")
	flag.IntVar(&redisDb, "db", 1, "redis db")
	flag.StringVar(&logLevel, "log-level", "info", "log level")

	flag.Parse()

	logger.Config(logLevel)

	rc := redis.NewClient(&redis.Options{Addr: redisHost, DB: redisDb})
	nodePool := nodepool.NewNodePool(rc)
	nameNode, err := namenode.New(nodePool, rc)
	if err != nil {
		panic(err)
	}

	healthController := api.NewHealth()
	apiController := api.NewNameNode(nameNode)
	metricsController := api.NewMetrics()

	app = http.NewApplication()
	app.Mount(healthController, apiController, metricsController)
	if err := app.Listen(addr); err != nil {
		panic(err)
	}
}
