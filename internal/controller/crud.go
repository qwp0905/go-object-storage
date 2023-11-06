package controller

import (
	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/service"
)

type crudController struct {
	*controllerImpl
	svc *service.CRUDService
}

func NewCRUDController(svc *service.CRUDService) *crudController {
	c := &crudController{
		controllerImpl: New("/api"),
		svc:            svc,
	}
	c.router.Get("/*", c.getObject)
	c.router.Post("/*", c.putObject)
	c.router.Put("/*", c.putObject)
	c.router.Delete("/*", c.deleteObject)

	return c
}

func (c *crudController) getObject(ctx *fiber.Ctx) error {
	obj, err := c.svc.GetObject(ctx.Path())
	if err != nil {
		return err
	}

	return ctx.SendStream(obj)
}

func (c *crudController) putObject(ctx *fiber.Ctx) error {
	if err := c.svc.PutObject(ctx.Path(), ctx.Request().BodyStream()); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}

func (c *crudController) deleteObject(ctx *fiber.Ctx) error {
	if err := c.svc.DeleteObject(ctx.Path()); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}
