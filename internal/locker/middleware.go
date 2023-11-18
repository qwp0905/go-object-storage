package locker

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

type Middleware struct {
	locker *RWMutex
}

func NewMiddleware(rc *redis.Client, key string, timeout time.Duration) (*Middleware, error) {
	locker, err := NewRWMutex(rc, key, timeout)
	if err != nil {
		return nil, err
	}

	return &Middleware{locker: locker}, nil
}

func (m *Middleware) RLock() func(*fiber.Ctx) error {
	return func(ctx *fiber.Ctx) error {
		if err := m.locker.RLock(ctx.Context()); err != nil {
			return err
		}
		defer m.locker.RUnlock(ctx.Context())

		if err := ctx.Next(); err != nil {
			return err
		}

		return nil
	}
}

func (m *Middleware) Lock() func(*fiber.Ctx) error {
	return func(ctx *fiber.Ctx) error {
		if err := m.locker.Lock(ctx.Context()); err != nil {
			return err
		}
		defer m.locker.Unlock(ctx.Context())

		if err := ctx.Next(); err != nil {
			return err
		}

		return nil
	}
}
