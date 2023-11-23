package locker

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/pkg/list"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/redis/go-redis/v9"
)

type LockerPool interface {
	Get(key string) RWMutex
}

type LockerPoolImpl struct {
	noCopy   nocopy.NoCopy
	rc       *redis.Client
	timeout  time.Duration
	lockers  map[string]*lockerPoolItem
	accessed *list.DoubleLinked[string]
	maxSize  int
	mu       *sync.Mutex
}

type lockerPoolItem struct {
	locker     RWMutex
	lastAccess *list.DoubleLinkedElement[string]
}

func NewPool(rc *redis.Client, timeout time.Duration) (LockerPool, error) {
	ctx := context.Background()
	for _, s := range []*redis.Script{
		readLockScript,
		readUnlockScript,
		writeLockScript,
		writeUnlockScript,
	} {
		ok, err := s.Exists(ctx, rc).Result()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if ok[0] {
			continue
		}
		if err := s.Load(ctx, rc).Err(); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return &LockerPoolImpl{
		rc:       rc,
		timeout:  timeout,
		lockers:  make(map[string]*lockerPoolItem),
		accessed: list.NewDoubleLinked[string](),
		maxSize:  500,
		mu:       new(sync.Mutex),
	}, nil
}

func (p *LockerPoolImpl) Get(key string) RWMutex {
	p.mu.Lock()
	defer p.mu.Unlock()

	item, ok := p.lockers[key]
	if ok {
		p.accessed.MoveBack(item.lastAccess)
		return item.locker
	}

	ni := &lockerPoolItem{
		lastAccess: list.NewDoubleLinkedElement[string](key),
		locker:     &rwMutexImpl{rc: p.rc, timeout: p.timeout, key: key},
	}

	for len(p.lockers) >= p.maxSize {
		l := p.accessed.First()
		p.accessed.Remove(l)
		delete(p.lockers, l.Value)
	}

	p.lockers[key] = ni
	p.accessed.PushBack(ni.lastAccess)
	return ni.locker
}
