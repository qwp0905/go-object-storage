package filesystem

import (
	"io"

	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func (f *FileSystem) GetObject(key string) (io.Reader, error) {
	return f.readFile(f.getDataKey(key))
}

func (f *FileSystem) PutObject(r io.Reader) (*datanode.Metadata, error) {
	source := f.generateKey()
	size, err := f.writeFile(f.getDataKey(source), r)
	if err != nil {
		return nil, err
	}

	return &datanode.Metadata{
		Source:    source,
		Size:      size,
		NodeId:    f.id,
		NextNodes: []*datanode.NextRoute{},
	}, nil
}

func (f *FileSystem) DeleteObject(key string) error {
	return f.removeFile(f.getDataKey(key))
}
