package namenode

import (
	"context"
	"io"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/locker"
	"github.com/qwp0905/go-object-storage/internal/metadata"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
	"github.com/redis/go-redis/v9"
)

type NameNode interface {
	HeadObject(ctx context.Context, key string) (*metadata.Metadata, error)
	GetObject(ctx context.Context, key string) (*metadata.Metadata, io.Reader, error)
	ListObject(ctx context.Context, prefix, delimiter, after string, limit int) (*ListObjectResult, error)
	PutObject(ctx context.Context, key, contentType string, size int, r io.Reader) error
	DeleteObject(ctx context.Context, key string) error
}

type nameNodeImpl struct {
	pool       nodepool.NodePool
	lockerPool locker.LockerPool
}

func New(pool nodepool.NodePool, rc *redis.Client) (*nameNodeImpl, error) {
	lp, err := locker.NewPool(rc, time.Second*30)
	if err != nil {
		return nil, err
	}
	return &nameNodeImpl{pool: pool, lockerPool: lp}, nil
}

func (n *nameNodeImpl) HeadObject(ctx context.Context, key string) (*metadata.Metadata, error) {
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

func (n *nameNodeImpl) GetObject(ctx context.Context, key string) (*metadata.Metadata, io.Reader, error) {
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

func (n *nameNodeImpl) ListObject(
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

	list := make([]ObjectList, len(l))
	for i, v := range l {
		list[i] = ObjectList{
			Size:         v.Size,
			LastModified: v.LastModified,
			Key:          v.Key,
			ContentType:  v.Type,
		}
	}

	return &ListObjectResult{Prefixes: p.Values(), List: list}, nil
}

func (n *nameNodeImpl) PutObject(ctx context.Context, key, contentType string, size int, r io.Reader) error {
	rootId, err := n.pool.GetRootId(ctx)
	if err != nil {
		return err
	}

	return n.put(ctx, key, rootId, n.pool.GetRootKey(), contentType, size, r)
}

func (n *nameNodeImpl) DeleteObject(ctx context.Context, key string) error {
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
