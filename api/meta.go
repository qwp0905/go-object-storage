package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

type meta struct {
	*controllerImpl
	svc datanode.DataNode
}

func NewMeta(svc datanode.DataNode) Controller {
	c := &meta{
		controllerImpl: newController("/meta"),
		svc:            svc,
	}

	c.router.Get("/*", c.get)
	c.router.Put("/", c.put)
	c.router.Delete("/*", c.delete)

	return c
}

func (c *meta) get(ctx *fiber.Ctx) error {
	out, err := c.svc.GetMetadata(c.getPath(ctx))
	if err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).JSON(out)
}

func (c *meta) put(ctx *fiber.Ctx) error {
	body := new(datanode.Metadata)
	if err := ctx.BodyParser(body); err != nil {
		return errors.WithStack(err)
	}

	if err := c.svc.PutMetadata(body); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}

func (c *meta) delete(ctx *fiber.Ctx) error {
	if err := c.svc.DeleteMetadata(c.getPath(ctx)); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}
