package http

import (
	"fmt"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/api"
	"github.com/qwp0905/go-object-storage/pkg/logger"
)

type Application struct {
	source *fiber.App
}

func NewApplication() *Application {
	source := fiber.New(fiber.Config{
		StreamRequestBody: true,
		JSONEncoder:       json.Marshal,
		JSONDecoder:       json.Unmarshal,
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError

			if e := new(fiber.Error); errors.As(err, &e) {
				code = e.Code
			}

			logger.CtxError(c, err)
			return c.Status(code).JSON(fiber.Map{"message": err.Error()})
		},
	})

	return &Application{source: source}
}

func (a *Application) Mount(controllers ...api.Controller) {
	for _, c := range controllers {
		a.source.Mount(c.Path(), c.Router())
	}
}

func (a *Application) Listen(port uint) error {
	return a.source.Listen(fmt.Sprintf(":%d", port))
}
