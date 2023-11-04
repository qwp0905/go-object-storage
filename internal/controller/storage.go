package controller

import "github.com/gofiber/fiber/v2"

func NewStorage() *storage {
	controller := &storage{
		controllerImpl: New("/storage"),
	}

	controller.router.Get("/*", controller.get)
	controller.router.Post("/*", controller.put)
	controller.router.Put("/*", controller.put)
	controller.router.Delete("/*", controller.delete)

	return controller
}

type storage struct {
	*controllerImpl
}

func (c *storage) get(ctx *fiber.Ctx) error {
	ctx.Path()
	return nil
}

func (c *storage) put(ctx *fiber.Ctx) error {
	return nil
}

func (c *storage) delete(ctx *fiber.Ctx) error {
	return nil
}
