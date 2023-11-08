package filesystem

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func (f *FileSystem) GetMetadata(key string) (io.Reader, error) {
	return f.readFile(f.getMetaKey(key))
}

func (f *FileSystem) PutMetadata(key string, metadata *datanode.Metadata) error {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(metadata); err != nil {
		return err
	}

	_, err := f.writeFile(f.getMetaKey(key), buf)
	if err != nil {
		return err
	}

	return nil
}

func (f *FileSystem) DeleteMetadata(key string) error {
	return f.removeFile(f.getMetaKey(key))
}
