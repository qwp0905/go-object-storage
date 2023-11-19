package api

import (
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metrics struct {
	*controllerImpl
}

func NewMetrics() Controller {
	c := &metrics{
		controllerImpl: newController("/metrics"),
	}

	c.router.Use(adaptor.HTTPHandler(promhttp.Handler()))
	return c
}
