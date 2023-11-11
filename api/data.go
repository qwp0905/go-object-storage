package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

type data struct {
	*controllerImpl
	svc *datanode.DataNode
}

func NewData(svc *datanode.DataNode) *data {
	c := &data{
		controllerImpl: New("/data"),
		svc:            svc,
	}

	c.router.Get("/*", c.get)
	c.router.Put("/", c.put)
	c.router.Delete("/*", c.delete)

	return c
}

func (c *data) get(ctx *fiber.Ctx) error {
	out, err := c.svc.GetObject(ctx.Context(), c.Path())
	if err != nil {
		return err
	}

	return ctx.SendStream(out)
}

func (c *data) put(ctx *fiber.Ctx) error {
	out, err := c.svc.PutObject(ctx.Request().Header.ContentLength(), ctx.Request().BodyStream())
	if err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).JSON(out)
}

func (c *data) delete(ctx *fiber.Ctx) error {
	if err := c.svc.DeleteObject(c.Path()); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}
