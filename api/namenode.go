package api

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/namenode"
)

type nameNode struct {
	*controllerImpl
	svc *namenode.NameNode
}

func NewNameNode(svc *namenode.NameNode) *nameNode {
	c := &nameNode{
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

func (c *nameNode) getObject(ctx *fiber.Ctx) error {
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

func (c *nameNode) listObject(ctx *fiber.Ctx) error {
	list, err := c.svc.ListObject(ctx.Context(), ctx.Query("prefix"), ctx.QueryInt("limit", 1000))
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

func (c *nameNode) putObject(ctx *fiber.Ctx) error {
	body := ctx.Request().BodyStream()
	if body == nil {
		return fiber.ErrBadRequest
	}
	// reader를 copy할 방법 모색 필요
	defer ctx.Request().CloseBodyStream()

	if err := c.svc.PutObject(ctx.Context(), c.getPath(ctx), ctx.Request().Header.ContentLength(), body); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}

func (c *nameNode) deleteObject(ctx *fiber.Ctx) error {
	if err := c.svc.DeleteObject(ctx.Context(), c.getPath(ctx)); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}

func (c *nameNode) headObject(ctx *fiber.Ctx) error {
	if _, err := c.svc.HeadObject(ctx.Context(), c.getPath(ctx)); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}
