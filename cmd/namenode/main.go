package main

import (
	"flag"

	"github.com/qwp0905/go-object-storage/api"
	"github.com/qwp0905/go-object-storage/internal/http"
	"github.com/qwp0905/go-object-storage/internal/namenode"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
)

var app *http.Application

var (
	addr  uint
	redis string
	db    int
)

func main() {
	flag.UintVar(&addr, "addr", 8080, "application addr")
	flag.StringVar(&redis, "redis", "localhost:6379", "redis host")
	flag.IntVar(&db, "db", 1, "redis db")

	flag.Parse()

	nodePool := nodepool.NewNodePool(redis, db)
	nameNode := namenode.New(nodePool)

	healthController := api.NewHealth()
	apiController := api.NewNameNode(nameNode)

	app = http.NewApplication(
		addr,
		healthController,
		apiController,
	)
	if err := app.Listen(); err != nil {
		panic(err)
	}
}
