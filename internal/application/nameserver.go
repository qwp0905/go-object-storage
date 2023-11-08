package application

import (
	"github.com/qwp0905/go-object-storage/internal/controller"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
)

type NameServer struct {
	*Application
}

func NewNameServer() *NameServer {
	nodePool := nodepool.NewNodePool("")

	healthController := controller.NewHealth()
	apiController := controller.NewApi(nodePool)

	return &NameServer{
		Application: New(healthController, apiController),
	}
}
