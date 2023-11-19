package http

import (
	"fmt"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/api"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

type Application interface {
	Mount(controllers ...api.Controller)
	Listen(port uint) error
}

type applicationImpl struct {
	nocopy nocopy.NoCopy
	source *fiber.App
}

func NewApplication() Application {
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

	return &applicationImpl{source: source}
}

func (a *applicationImpl) Mount(controllers ...api.Controller) {
	for _, c := range controllers {
		a.source.Mount(c.Path(), c.Router())
	}
}

func (a *applicationImpl) Listen(port uint) error {
	return a.source.Listen(fmt.Sprintf(":%d", port))
}
