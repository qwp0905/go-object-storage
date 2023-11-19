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

type NameNode interface {
	HeadObject(ctx context.Context, key string) (*datanode.Metadata, error)
	GetObject(ctx context.Context, key string) (*datanode.Metadata, io.Reader, error)
	ListObject(ctx context.Context, prefix, delimiter, after string, limit int) (*ListObjectResult, error)
	PutObject(ctx context.Context, key, contentType string, size int, r io.Reader) error
	DeleteObject(ctx context.Context, key string) error
}

type NameNodeImpl struct {
	pool       nodepool.NodePool
	lockerPool locker.LockerPool
}

func New(pool nodepool.NodePool, rc *redis.Client) (*NameNodeImpl, error) {
	lp, err := locker.NewPool(rc, time.Second*30)
	if err != nil {
		return nil, err
	}
	return &NameNodeImpl{pool: pool, lockerPool: lp}, nil
}

func (n *NameNodeImpl) HeadObject(ctx context.Context, key string) (*datanode.Metadata, error) {
	rootId, err := n.pool.GetRootId(ctx)
	if err != nil {
		return nil, err
	}

	metadata, err := n.get(ctx, key, rootId, n.pool.GetRootKey())
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func (n *NameNodeImpl) GetObject(ctx context.Context, key string) (*datanode.Metadata, io.Reader, error) {
	metadata, err := n.HeadObject(ctx, key)
	if err != nil {
		return nil, nil, err
	}

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

func (n *NameNodeImpl) ListObject(
	ctx context.Context,
	prefix, delimiter, after string,
	limit int,
) (*ListObjectResult, error) {
	rootId, err := n.pool.GetRootId(ctx)
	if err != nil {
		return nil, err
	}

	p, l, err := n.scan(ctx, prefix, delimiter, after, limit, rootId, n.pool.GetRootKey())
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

func (n *NameNodeImpl) PutObject(ctx context.Context, key, contentType string, size int, r io.Reader) error {
	rootId, err := n.pool.GetRootId(ctx)
	if err != nil {
		return err
	}

	return n.put(ctx, key, rootId, n.pool.GetRootKey(), contentType, size, r)
}

func (n *NameNodeImpl) DeleteObject(ctx context.Context, key string) error {
	metadata, err := n.HeadObject(ctx, key)
	if err != nil {
		if err == fiber.ErrNotFound {
			return nil
		}
		return err
	}

	rootId, err := n.pool.GetRootId(ctx)
	if err != nil {
		return err
	}

	if _, err := n.delete(ctx, key, rootId, n.pool.GetRootKey()); err != nil {
		return err
	}

	return n.pool.DeleteDirect(ctx, metadata)
}

func generateKey() string {
	id, _ := uuid.NewRandom()
	return id.String()
}
