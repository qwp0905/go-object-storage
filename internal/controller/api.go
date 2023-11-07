package controller

import (
	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/service"
)

type apiController struct {
	*controllerImpl
	svc *service.ApiService
}

func NewApiController(svc *service.ApiService) *apiController {
	c := &apiController{
		controllerImpl: New("/api"),
		svc:            svc,
	}
	c.router.Get("/", c.listObject)
	c.router.Get("/*", c.getObject)
	c.router.Post("/*", c.putObject)
	c.router.Put("/*", c.putObject)
	c.router.Delete("/*", c.deleteObject)

	return c
}

func (c *apiController) getObject(ctx *fiber.Ctx) error {
	obj, err := c.svc.GetObject(ctx.Path())
	if err != nil {
		return err
	}

	return ctx.SendStream(obj)
}

type listObjectResponse struct {
	Key          string `json:"key"`
	Size         string `json:"size"`
	LastModified string `json:"last_modified"`
}

func (c *apiController) listObject(ctx *fiber.Ctx) error {
	list, err := c.svc.ListObject(ctx.Query("prefix"))
	if err != nil {
		return err
	}

	out := []*listObjectResponse{}
	for _, meta := range list {
		out = append(out, &listObjectResponse{
			Key:  meta.Key,
			Size: meta.Size,
		})
	}

	return ctx.Status(fiber.StatusOK).JSON(out)
}

func (c *apiController) putObject(ctx *fiber.Ctx) error {
	if err := c.svc.PutObject(ctx.Path(), ctx.Request().BodyStream()); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}

func (c *apiController) deleteObject(ctx *fiber.Ctx) error {
	if err := c.svc.DeleteObject(ctx.Path()); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}
