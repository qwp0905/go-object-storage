package filesystem

import (
	"fmt"
	"io"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
)

type FileSystem interface {
	ReadFile(key string) (*os.File, int, error)
	WriteFile(key string, r io.Reader) (uint, error)
	RemoveFile(key string) error
}

type fileSystemImpl struct {
	basedir string
}

func NewFileSystem(basedir string) FileSystem {
	return &fileSystemImpl{basedir: basedir}
}

func (f *fileSystemImpl) path(key string) string {
	return fmt.Sprintf("%s/%s", f.basedir, key)
}

func (f *fileSystemImpl) ReadFile(key string) (*os.File, int, error) {
	file, err := os.Open(f.path(key))
	if os.IsNotExist(err) {
		return nil, 0, errors.WithStack(fiber.ErrNotFound)
	}
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}

	return file, int(info.Size()), nil
}

func (f *fileSystemImpl) WriteFile(key string, r io.Reader) (uint, error) {
	file, err := os.Create(f.path(key))
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

func (f *fileSystemImpl) RemoveFile(key string) error {
	if err := os.Remove(f.path(key)); err != nil && !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	return nil
}

func EnsureDir(path string) error {
	err := os.Mkdir(path, os.ModeDir)
	if err == nil {
		return nil
	}
	if !os.IsExist(err) {
		return errors.WithStack(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return errors.WithStack(err)
	}
	if !info.IsDir() {
		return errors.Errorf("path %s is not a directory", path)
	}
	return nil
}
