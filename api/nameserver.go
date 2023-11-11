package api

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
)

type nameserver struct {
	*controllerImpl
	svc *nodepool.NodePool
}

func NewNameServer(svc *nodepool.NodePool) *nameserver {
	c := &nameserver{
		controllerImpl: New("/api"),
		svc:            svc,
	}

	c.router.Head("/*", c.headObject)
	c.router.Get("/", c.listObject)
	c.router.Get("/*", c.getObject)
	c.router.Post("/*", c.putObject)
	c.router.Put("/*", c.putObject)
	c.router.Delete("/*", c.deleteObject)

	return c
}

func (c *nameserver) getObject(ctx *fiber.Ctx) error {
	obj, err := c.svc.GetObject(ctx.Context(), c.getPath(ctx))
	if err != nil {
		return err
	}

	return ctx.SendStream(obj)
}

type listObjectResponse struct {
	Key          string    `json:"key"`
	Size         uint      `json:"size"`
	LastModified time.Time `json:"last_modified"`
}

func (c *nameserver) listObject(ctx *fiber.Ctx) error {
	list, err := c.svc.ListObject(ctx.Query("prefix"))
	if err != nil {
		return err
	}

	out := []*listObjectResponse{}
	for _, meta := range list {
		out = append(out, &listObjectResponse{
			Key:          meta.Key,
			Size:         meta.Size,
			LastModified: meta.LastModified,
		})
	}

	return ctx.Status(fiber.StatusOK).JSON(out)
}

func (c *nameserver) putObject(ctx *fiber.Ctx) error {
	body := ctx.Request().BodyStream()
	if body == nil {
		return fiber.ErrBadRequest
	}
	defer ctx.Request().CloseBodyStream()

	if err := c.svc.PutObject(c.getPath(ctx), ctx.Request().Header.ContentLength(), body); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}

func (c *nameserver) deleteObject(ctx *fiber.Ctx) error {
	if err := c.svc.DeleteObject(c.getPath(ctx)); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}

func (c *nameserver) headObject(ctx *fiber.Ctx) error {
	if _, err := c.svc.GetMetadata(c.getPath(ctx)); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}
