package api

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
)

type node struct {
	*controllerImpl
	svc *nodepool.NodePool
}

func NewNode(svc *nodepool.NodePool) *node {
	controller := &node{
		controllerImpl: New("/node"),
		svc:            svc,
	}

	controller.router.Post("/register", controller.registerNode)
	return controller
}

type registerNodeBody struct {
	Id   string `json:"id"`
	Host string `json:"host"`
	Port uint   `json:"port"`
}

func (c *node) registerNode(ctx *fiber.Ctx) error {
	body := new(registerNodeBody)
	if err := ctx.BodyParser(body); err != nil {
		return errors.WithStack(err)
	}

	if err := c.svc.Register(body.Id, fmt.Sprintf("%s:%d", body.Host, body.Port)); err != nil {
		return err
	}

	return ctx.Status(fiber.StatusOK).SendString("OK")
}
