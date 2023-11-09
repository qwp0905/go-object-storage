package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

type meta struct {
	*controllerImpl
	svc *datanode.DataNode
}

func NewMeta(svc *datanode.DataNode) *meta {
	controller := &meta{
		controllerImpl: New("/meta"),
		svc:            svc,
	}

	controller.router.Head("/*")
	controller.router.Get("/*", controller.get)
	controller.router.Put("/*", controller.put)
	controller.router.Delete("/*", controller.delete)

	return controller
}

func (c *meta) get(ctx *fiber.Ctx) error {
	out, err := c.svc.GetMetadata(c.Path())
	if err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).JSON(out)
}

func (c *meta) put(ctx *fiber.Ctx) error {
	body := new(datanode.Metadata)
	if err := ctx.BodyParser(body); err != nil {
		return err
	}

	if err := c.svc.PutMetadata(c.Path(), body); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}

func (c *meta) delete(ctx *fiber.Ctx) error {
	if err := c.svc.DeleteMetadata(c.Path()); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}
