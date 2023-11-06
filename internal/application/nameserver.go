package application

import (
	"github.com/qwp0905/go-object-storage/internal/controller"
)

type NameServer struct {
	*Application
}

func NewNameServer() *NameServer {
	healthController := controller.NewHealth()

	return &NameServer{
		Application: New(healthController),
	}
}
