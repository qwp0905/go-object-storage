package filesystem

import (
	"context"
	"io"
	"os"

	"github.com/gofiber/fiber/v2"
)

type FileSystem struct{}

func NewFileSystem() *FileSystem {
	return &FileSystem{}
}

func (f *FileSystem) ReadFile(ctx context.Context, key string) (*os.File, error) {
	file, err := os.Open(key)
	if os.IsNotExist(err) {
		return nil, fiber.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	go closeFile(ctx, file)

	return file, nil
}

func (f *FileSystem) WriteFile(key string, r io.Reader) (uint, error) {
	file, err := os.Create(key)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	n, err := io.Copy(file, r)
	if err != nil {
		return 0, err
	}

	return uint(n), nil
}

func (f *FileSystem) RemoveFile(key string) error {
	if err := os.Remove(key); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func closeFile(ctx context.Context, file *os.File) {
	defer file.Close()
	<-ctx.Done()
}
