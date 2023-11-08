package main

import (
	"flag"

	"github.com/qwp0905/go-object-storage/api"
	"github.com/qwp0905/go-object-storage/internal/http"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
)

var app *http.Application

var (
	rootKey string
	addr    uint
)

func init() {
	flag.StringVar(&rootKey, "root", "/", "root metadata key")
	flag.UintVar(&addr, "addr", 8080, "application addr")

	flag.Parse()

	nodePool := nodepool.NewNodePool(rootKey)

	healthController := api.NewHealth()
	apiController := api.NewNameServer(nodePool)
	nodeController := api.NewNode(nodePool)

	app = http.NewApplication(
		healthController,
		apiController,
		nodeController,
	)
}

func main() {
	if err := app.Listen(addr); err != nil {
		panic(err)
	}
}
