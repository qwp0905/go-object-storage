package application

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/controller"
	"github.com/qwp0905/go-object-storage/pkg/logger"
)

type Application struct {
	source *fiber.App
}

func New(controllers ...controller.Controller) *Application {
	source := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError

			if e := new(fiber.Error); errors.As(err, &e) {
				code = e.Code
			}

			logger.Errorf("%+v", err)
			return c.Status(code).JSON(fiber.Map{"message": err.Error()})
		},
	})

	for _, c := range controllers {
		source.Mount(c.Path(), c.Router())
	}

	return &Application{source: source}
}

func (a *Application) Listen(port uint) error {
	return a.source.Listen(fmt.Sprintf(":%d", port))
}
