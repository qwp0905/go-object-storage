package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/goccy/go-json"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/bufferpool"
	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/pkg/list"
)

type Operation string

const (
	OperationPut = Operation("PUT")
	OperationDel = Operation("DEL")
)

type Log struct {
	Operation Operation `json:"operation"`
	Key       string    `json:"key"`
	Index     uint64    `json:"index"`
}

type LogStore interface {
	Insert(log *Log)
	Uncommitted() []*Log
	Commit(lastIndex uint64) error
	LastIndex() uint64
}

type logStoreImpl struct {
	uncommitted *list.Queue[*Log]
	committed   uint64
	mu          *sync.RWMutex
	bp          bufferpool.BufferPool
}

func NewLogStore(basedir string, bp bufferpool.BufferPool) (LogStore, error) {
	if err := filesystem.EnsureDir(fmt.Sprintf("%s/log", basedir)); err != nil {
		return nil, err
	}
	l := &logStoreImpl{
		uncommitted: list.NewQueue[*Log](),
		mu:          new(sync.RWMutex),
		bp:          bp,
		committed:   0,
	}
	b, err := os.ReadFile(l.lastIndexPath())
	if err != nil {
		if os.IsNotExist(err) {
			return l, nil
		}
		return nil, errors.WithStack(err)
	}
	l.committed = binary.LittleEndian.Uint64(b)

	return l, nil
}

func (l *logStoreImpl) lastIndexPath() string {
	return fmt.Sprintf("log/%d", -1)
}

func (l *logStoreImpl) path(index uint64) string {
	return fmt.Sprintf("log/%d", index)
}

func (l *logStoreImpl) Insert(log *Log) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.uncommitted.Push(log)
}

func (l *logStoreImpl) Uncommitted() []*Log {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return *l.uncommitted
}

func (l *logStoreImpl) Commit(lastIndex uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	list := make([]*Log, 0)
	for l.uncommitted.Len() > 0 && l.uncommitted.First().Index <= lastIndex {
		list = append(list, l.uncommitted.Shift())
	}
	b, err := json.Marshal(list)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := l.bp.Put(l.path(lastIndex), len(b), bytes.NewReader(b)); err != nil {
		return err
	}
	lb := make([]byte, 8)
	binary.LittleEndian.PutUint64(lb, uint64(lastIndex))
	if err := os.WriteFile(l.lastIndexPath(), lb, 0666); err != nil {
		return errors.WithStack(err)
	}

	l.committed = lastIndex
	return nil
}

func (l *logStoreImpl) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.uncommitted.Len() == 0 {
		return l.committed
	}
	return l.uncommitted.Last().Index
}
