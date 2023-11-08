package api

import "github.com/gofiber/fiber/v2"

func NewHealth() *health {
	controller := &health{
		controllerImpl: New("/health"),
	}

	controller.router.Get("/", controller.check)

	return controller
}

type health struct {
	*controllerImpl
}

func (c *health) check(ctx *fiber.Ctx) error {
	return ctx.Status(200).JSON(fiber.Map{"status": "up"})
}
