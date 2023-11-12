package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/qwp0905/go-object-storage/api"
	"github.com/qwp0905/go-object-storage/internal/bufferpool"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/internal/http"
	"github.com/qwp0905/go-object-storage/pkg/logger"
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
	logger.Infof("%01f mb can be allocate", float64(os.Getpagesize())*0.8)
	node, addr, err := datanode.NewDataNode(configPath, bp)
	if err != nil {
		panic(err)
	}

	dataController := api.NewData(node)
	metaController := api.NewMeta(node)

	app = http.NewApplication(addr, dataController, metaController)
	go node.Register()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	done := make(chan struct{}, 1)
	go graceful(bp, sigs, done)

	if err := app.Listen(); err != nil {
		panic(err)
	}
	<-done
}

func graceful(bp *bufferpool.BufferPool, sig chan os.Signal, done chan struct{}) {
	<-sig
	defer func() {
		logger.Info("data all flushed")
		done <- struct{}{}
	}()
	if err := bp.FlushAll(); err != nil {
		panic(err)
	}
}
