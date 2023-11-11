package http

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/api"
	"github.com/qwp0905/go-object-storage/pkg/logger"
)

type Application struct {
	source *fiber.App
	port   uint
}

func NewApplication(port uint, controllers ...api.Controller) *Application {
	source := fiber.New(fiber.Config{
		StreamRequestBody: true,
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError

			if e := new(fiber.Error); errors.As(err, &e) {
				code = e.Code
			}

			logger.FromCtx(c, err)
			return c.Status(code).JSON(fiber.Map{"message": err.Error()})
		},
	})

	for _, c := range controllers {
		source.Mount(c.Path(), c.Router())
	}

	return &Application{source: source, port: port}
}

func (a *Application) Listen() error {
	return a.source.Listen(fmt.Sprintf(":%d", a.port))
}
