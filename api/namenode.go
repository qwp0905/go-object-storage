package api

import (
	"bytes"
	"fmt"
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
	meta, obj, err := c.svc.GetObject(ctx.Context(), c.getPath(ctx))
	if err != nil {
		return err
	}
	ctx.Set("Content-Type", meta.Type)
	ctx.Set("Last-Modified", meta.LastModified.Format(time.RFC1123))

	return ctx.SendStream(obj, int(meta.Size))
}

func (c *nameNode) listObject(ctx *fiber.Ctx) error {
	list, err := c.svc.ListObject(
		ctx.Context(),
		ctx.Query("prefix"),
		ctx.Query("delimiter"),
		ctx.Query("after"),
		ctx.QueryInt("limit", 1000),
	)
	if err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).JSON(list)
}

func (c *nameNode) putObject(ctx *fiber.Ctx) error {
	body := bytes.NewReader(ctx.BodyRaw())
	if err := c.svc.PutObject(
		ctx.Context(),
		c.getPath(ctx),
		ctx.Get("Content-Type", "text/plain"),
		ctx.Request().Header.ContentLength(),
		body,
	); err != nil {
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
	meta, err := c.svc.HeadObject(ctx.Context(), c.getPath(ctx))
	if err != nil {
		return err
	}
	ctx.Set("Content-Type", meta.Type)
	ctx.Set("Content-Length", fmt.Sprintf("%d", meta.Size))
	ctx.Set("Last-Modified", meta.LastModified.Format(time.RFC1123))

	return ctx.Status(fiber.StatusOK).Send(nil)
}
