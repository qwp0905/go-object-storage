package api

import (
	"github.com/gofiber/fiber/v2"
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
}

func (c *node) registerNode(ctx *fiber.Ctx) error {
	body := new(registerNodeBody)
	if err := ctx.BodyParser(body); err != nil {
		return err
	}

	return c.svc.Register(body.Id, body.Host)
}
