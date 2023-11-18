package namenode

import (
	"context"
	"io"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/internal/locker"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
	"github.com/redis/go-redis/v9"
)

type NameNode struct {
	pool   *nodepool.NodePool
	locker *locker.RWMutex
}

func New(pool *nodepool.NodePool, rc *redis.Client) (*NameNode, error) {
	locker, err := locker.NewRWMutex(rc, "namenode", time.Second*30)
	if err != nil {
		return nil, err
	}
	return &NameNode{pool: pool, locker: locker}, nil
}

func (n *NameNode) HeadObject(ctx context.Context, key string) (*datanode.Metadata, error) {
	if err := n.locker.RLock(ctx); err != nil {
		return nil, err
	}
	defer n.locker.RUnlock(ctx)

	root, err := n.pool.GetRootMetadata(ctx)
	if err != nil {
		return nil, err
	}

	_, metadata, err := n.get(ctx, n.pool.GetRootId(), key, root)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func (n *NameNode) GetObject(ctx context.Context, key string) (*datanode.Metadata, io.Reader, error) {
	metadata, err := n.HeadObject(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	if err := n.locker.RLock(ctx); err != nil {
		return nil, nil, err
	}
	defer n.locker.RUnlock(ctx)

	r, err := n.pool.GetDirect(ctx, metadata)
	if err != nil {
		return nil, nil, err
	}

	return metadata, r, nil
}

type ListObjectResult struct {
	Prefixes []string     `json:"prefixes,omitempty"`
	List     []ObjectList `json:"list,omitempty"`
}

type ObjectList struct {
	Key          string    `json:"key"`
	Size         uint      `json:"size"`
	LastModified time.Time `json:"last_modified"`
	ContentType  string    `json:"content-type"`
}

func (n *NameNode) ListObject(
	ctx context.Context,
	prefix, delimiter, after string,
	limit int,
) (*ListObjectResult, error) {
	if err := n.locker.RLock(ctx); err != nil {
		return nil, err
	}
	defer n.locker.RUnlock(ctx)

	root, err := n.pool.GetRootMetadata(ctx)
	if err != nil {
		return nil, err
	}

	p, l, err := n.scan(ctx, prefix, delimiter, after, limit, root)
	if err != nil {
		return nil, err
	}

	prefixes := make([]string, 0)
	for k := range p {
		prefixes = append(prefixes, k)
	}
	list := make([]ObjectList, len(l))
	for i, v := range l {
		list[i] = ObjectList{
			Size:         v.Size,
			LastModified: v.LastModified,
			Key:          v.Key,
			ContentType:  v.Type,
		}
	}

	return &ListObjectResult{Prefixes: prefixes, List: list}, nil
}

func (n *NameNode) PutObject(ctx context.Context, key, contentType string, size int, r io.Reader) error {
	if err := n.locker.Lock(ctx); err != nil {
		return err
	}
	defer n.locker.Unlock(ctx)

	root, err := n.pool.GetRootMetadata(ctx)
	if err != nil {
		return err
	}

	return n.put(ctx, n.pool.GetRootId(), key, contentType, root, size, r)
}

func (n *NameNode) DeleteObject(ctx context.Context, key string) error {
	root, err := n.pool.GetRootMetadata(ctx)
	if err != nil {
		return err
	}

	metadata, err := n.HeadObject(ctx, key)
	if err != nil {
		if err == fiber.ErrNotFound {
			return nil
		}
		return err
	}

	if err := n.locker.Lock(ctx); err != nil {
		return err
	}
	defer n.locker.Unlock(ctx)

	if _, err := n.delete(ctx, n.pool.GetRootId(), key, root); err != nil {
		return err
	}

	return n.pool.DeleteDirect(ctx, metadata)
}

func generateKey() string {
	id, _ := uuid.NewRandom()
	return id.String()
}
