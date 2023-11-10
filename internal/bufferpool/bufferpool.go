package bufferpool

import (
	"context"
	"io"
	"sync"

	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

const (
	B  = 1
	KB = B * (1 << 10)
	MB = KB * (1 << 10)
	GB = MB * (1 << 10)
	TB = GB * (1 << 10)
	PB = TB * (1 << 10)
)

type BufferPool struct {
	noCopy  nocopy.NoCopy
	fs      *filesystem.FileSystem
	locker  *sync.Mutex
	objects map[string]*buffer
	maxSize uint
}

func NewBufferPool(maxSize uint, fs *filesystem.FileSystem) *BufferPool {
	return &BufferPool{
		fs:      fs,
		locker:  new(sync.Mutex),
		maxSize: maxSize,
	}
}

func (p *BufferPool) GetObject(ctx context.Context, key string) (io.Reader, error) {
	obj, ok := p.objects[key]
	if ok {
		return obj.getData(), nil
	}

	buf, err := p.newBuffer(ctx, key)
	if err != nil {
		return nil, err
	}

	return buf.getData(), nil
}

func (p *BufferPool) PutObject(ctx context.Context, key string) {
}
