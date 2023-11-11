package filesystem

import (
	"io"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
)

type FileSystem struct{}

func NewFileSystem() *FileSystem {
	return &FileSystem{}
}

func (f *FileSystem) ReadFile(key string) (*os.File, error) {
	file, err := os.Open(key)
	if os.IsNotExist(err) {
		return nil, fiber.ErrNotFound
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return file, nil
}

func (f *FileSystem) WriteFile(key string, r io.Reader) (uint, error) {
	file, err := os.Create(key)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer file.Close()

	n, err := io.Copy(file, r)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return uint(n), nil
}

func (f *FileSystem) RemoveFile(key string) error {
	if err := os.Remove(key); err != nil && !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	return nil
}
