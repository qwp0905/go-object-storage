package main

import (
	"flag"
	"os"

	"github.com/qwp0905/go-object-storage/api"
	"github.com/qwp0905/go-object-storage/internal/bufferpool"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/internal/http"
)

var app *http.Application

var (
	configPath string
)

func main() {
	flag.StringVar(&configPath, "config", "/var/lib/datanode/config.yaml", "config file path")
	flag.Parse()

	fs := filesystem.NewFileSystem()
	bp := bufferpool.NewBufferPool(int(float64(os.Getpagesize()*bufferpool.MB)*0.8), fs)
	node, addr, err := datanode.NewDataNode(configPath, bp)
	if err != nil {
		panic(err)
	}

	dataController := api.NewData(node)
	metaController := api.NewMeta(node)

	app = http.NewApplication(addr, dataController, metaController)
	go node.Register()
	if err := app.Listen(); err != nil {
		panic(err)
	}
}
