package filesystem

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

type FileSystem struct {
	base string
	id   string
}

func NewFileSystem(base string) *FileSystem {
	return &FileSystem{base: base}
}

func (f *FileSystem) readFile(key string) (io.Reader, error) {
	file, err := os.Open(key)
	if os.IsNotExist(err) {
		return nil, fiber.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (f *FileSystem) writeFile(key string, r io.Reader) (uint, error) {
	file, err := os.Create(key)
	if err != nil {
		return 0, err
	}

	n, err := io.Copy(file, r)
	if err != nil {
		return 0, err
	}

	return uint(n), nil
}

func (f *FileSystem) removeFile(key string) error {
	if err := os.Remove(key); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (f *FileSystem) getMetaKey(key string) string {
	return fmt.Sprintf(
		"%s/meta/%s",
		f.base,
		base64.StdEncoding.EncodeToString([]byte(key)),
	)
}

func (f *FileSystem) getDataKey(key string) string {
	return fmt.Sprintf("%s/object/%s", f.base, key)
}

func (f *FileSystem) generateKey() string {
	id, _ := uuid.NewRandom()
	return id.String()
}
