package api

import (
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metrics struct {
	*controllerImpl
}

func NewMetrics() *metrics {
	c := &metrics{
		controllerImpl: New("/metrics"),
	}

	c.router.Use(adaptor.HTTPHandler(promhttp.Handler()))
	return c
}
