package nodepool

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/internal/metadata"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

type NodePool interface {
	FindInCache(key string) (string, string)
	GetNodeHost(ctx context.Context, id string) (string, error)
	GetNodeIds(ctx context.Context) ([]string, error)
	AcquireNode(ctx context.Context) (string, error)
	GetMetadata(ctx context.Context, id, key string) (*metadata.Metadata, error)
	PutMetadata(ctx context.Context, id string, metadata *metadata.Metadata) error
	DeleteMetadata(ctx context.Context, id, key string) error
	PutDirect(ctx context.Context, metadata *metadata.Metadata, r io.Reader) error
	GetDirect(ctx context.Context, metadata *metadata.Metadata) (io.Reader, error)
	DeleteDirect(ctx context.Context, metadata *metadata.Metadata) error
}

type nodePoolImpl struct {
	noCopy  nocopy.NoCopy
	client  *fasthttp.Client
	counter func(int) int
	rc      *redis.Client
	cache   Cache
}

type NodeInfo struct {
	Id string `json:"id"`
}

func NewNodePool(rc *redis.Client) NodePool {
	return &nodePoolImpl{
		client:  &fasthttp.Client{MaxConnsPerHost: 1024},
		counter: counter(),
		rc:      rc,
		cache:   NewCache(100),
	}
}

func (p *nodePoolImpl) GetNodeIds(ctx context.Context) ([]string, error) {
	ids, err := p.rc.Keys(ctx, datanode.HostKey("*")).Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	for i := range ids {
		ids[i] = datanode.IdFromKey(ids[i])
	}
	return ids, nil
}

func (p *nodePoolImpl) GetNodeHost(ctx context.Context, id string) (string, error) {
	host, err := p.rc.Get(ctx, datanode.HostKey(id)).Result()
	if err != nil {
		return "", errors.WithStack(err)
	}

	return host, nil
}

func (p *nodePoolImpl) AcquireNode(ctx context.Context) (string, error) {
	ids, err := p.rc.Keys(ctx, datanode.HostKey("*")).Result()
	if err != nil {
		return "", errors.WithStack(err)
	}

	if len(ids) == 0 {
		return "", errors.New("no datanode registered...")
	}

	return datanode.IdFromKey(ids[p.counter(len(ids))]), nil
}

func (p *nodePoolImpl) FindInCache(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if id := p.cache.Get(key[:len(key)-i]); id != "" {
			return id, key[:len(key)-i]
		}
	}

	return "", ""
}
