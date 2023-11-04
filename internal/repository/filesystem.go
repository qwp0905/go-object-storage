package repository

import (
	"bufio"
	"io"
	"os"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func NewFileSystem(base string) *FileSystem {
	return &FileSystem{base: base}
}

type FileSystem struct {
	base string
}

func genID() string {
	id, _ := uuid.NewRandom()
	return id.String()
}

func (f *FileSystem) Get(key string) (io.Reader, error) {
	r, err := os.Open(f.base + key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return bufio.NewReader(r), nil
}

func (f *FileSystem) Create(file io.Reader) (string, error) {
	key := genID()

	w, err := os.Create(f.base + key)
	if err != nil {
		return "", errors.WithStack(err)
	}
	if _, err := io.Copy(w, file); err != nil {
		return "", errors.WithStack(err)
	}

	return key, nil
}
