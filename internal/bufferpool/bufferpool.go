package bufferpool

import (
	"sync"

	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
)

const (
	B  = uint(1)
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
	maxSize uint
}

func NewBufferPool(maxSize uint, fs *filesystem.FileSystem) *BufferPool {
	return &BufferPool{
		fs:      fs,
		locker:  new(sync.Mutex),
		maxSize: maxSize,
	}
}
