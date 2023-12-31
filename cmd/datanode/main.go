package main

import (
	"flag"
	"fmt"
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

var app http.Application

var (
	redisHost string
	baseDir   string
	redisDB   int
	addr      uint
	host      string
	logLevel  string
)

func main() {
	flag.StringVar(&redisHost, "redis", "localhost:6379", "redis host")
	flag.IntVar(&redisDB, "db", 1, "redis db no")
	flag.StringVar(&baseDir, "base", "/var/lib/datanode/", "base directory")
	flag.StringVar(&host, "host", "", "host which to be register in redis")
	flag.UintVar(&addr, "addr", 8080, "application port")
	flag.StringVar(&logLevel, "log-level", "info", "log level")

	flag.Parse()

	logger.Config(logLevel)

	fs := filesystem.NewFileSystem(baseDir)
	bp := bufferpool.NewBufferPool(int(float64(os.Getpagesize()*bufferpool.MB)*0.8), fs)
	logger.Infof("%01f mb can be allocate", float64(os.Getpagesize())*0.8)

	node, err := datanode.NewDataNode(baseDir, &datanode.Config{
		RedisHost: redisHost,
		RedisDB:   redisDB,
		Host:      fmt.Sprintf("%s:%d", host, addr),
	}, bp)
	if err != nil {
		panic(err)
	}
	go node.Live()

	dataController := api.NewData(node)
	metaController := api.NewMeta(node)
	healthController := api.NewHealth()
	metricsController := api.NewMetrics()

	app = http.NewApplication()
	app.Mount(
		dataController,
		metaController,
		healthController,
		metricsController,
	)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	done := make(chan struct{}, 1)
	go bp.BeforeDestroy(sigs, done)

	if err := app.Listen(addr); err != nil {
		panic(err)
	}
	<-done
}
