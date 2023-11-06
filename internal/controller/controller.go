package controller

import (
	"github.com/gofiber/fiber/v2"
)

type Controller interface {
	Path() string
	Router() *fiber.App
}

type controllerImpl struct {
	path   string
	router *fiber.App
}

func New(path string) *controllerImpl {
	return &controllerImpl{path: path, router: fiber.New()}
}

func (c *controllerImpl) Path() string {
	return c.path
}

func (c *controllerImpl) Router() *fiber.App {
	return c.router
}
