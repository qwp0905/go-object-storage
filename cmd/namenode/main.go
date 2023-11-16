package main

import (
	"flag"

	"github.com/qwp0905/go-object-storage/api"
	"github.com/qwp0905/go-object-storage/internal/http"
	"github.com/qwp0905/go-object-storage/internal/namenode"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
	"github.com/redis/go-redis/v9"
)

var app *http.Application

var (
	addr      uint
	redisHost string
	redisDb   int
	master    bool
)

func main() {
	flag.UintVar(&addr, "addr", 8080, "application addr")
	flag.StringVar(&redisHost, "redis", "localhost:6379", "redis host")
	flag.IntVar(&redisDb, "db", 1, "redis db")
	flag.BoolVar(&master, "master", false, "is master node to check datanode health")

	flag.Parse()

	rc := redis.NewClient(&redis.Options{Addr: redisHost, DB: redisDb})
	nodePool := nodepool.NewNodePool(rc)
	nameNode := namenode.New(nodePool, rc)
	if master {
		go nodePool.CheckAliveNodes()
	}

	healthController := api.NewHealth()
	apiController := api.NewNameNode(nameNode)

	app = http.NewApplication()
	app.Mount(healthController, apiController)
	if err := app.Listen(addr); err != nil {
		panic(err)
	}
}
