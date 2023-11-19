package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

type data struct {
	*controllerImpl
	svc datanode.DataNode
}

func NewData(svc datanode.DataNode) Controller {
	c := &data{
		controllerImpl: newController("/data"),
		svc:            svc,
	}

	c.router.Get("/:key", c.get)
	c.router.Put("/:key", c.put)
	c.router.Delete("/:key", c.delete)

	return c
}

func (c *data) get(ctx *fiber.Ctx) error {
	out, err := c.svc.GetObject(ctx.Context(), ctx.Params("key"))
	if err != nil {
		return err
	}

	return ctx.SendStream(out)
}

func (c *data) put(ctx *fiber.Ctx) error {
	body := ctx.Request().BodyStream()
	if body == nil {
		return fiber.ErrBadRequest
	}
	defer ctx.Request().CloseBodyStream()

	out, err := c.svc.PutObject(ctx.Params("key"), ctx.Request().Header.ContentLength(), body)
	if err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).JSON(out)
}

func (c *data) delete(ctx *fiber.Ctx) error {
	if err := c.svc.DeleteObject(ctx.Params("key")); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}
