package nodepool

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

type NodePool interface {
	GetRootId(ctx context.Context) (string, error)
	GetRootKey() string
	GetNodeHost(ctx context.Context, id string) (string, error)
	AcquireNode(ctx context.Context) (string, error)
	GetMetadata(ctx context.Context, id, key string) (*datanode.Metadata, error)
	PutMetadata(ctx context.Context, id string, metadata *datanode.Metadata) error
	DeleteMetadata(ctx context.Context, id, key string) error
	PutDirect(ctx context.Context, metadata *datanode.Metadata, r io.Reader) (*datanode.Metadata, error)
	GetDirect(ctx context.Context, metadata *datanode.Metadata) (io.Reader, error)
	DeleteDirect(ctx context.Context, metadata *datanode.Metadata) error
}

type nodePoolImpl struct {
	noCopy  nocopy.NoCopy
	client  *fasthttp.Client
	root    *NodeInfo
	rootKey string
	counter func(int) int
	rc      *redis.Client
}

type NodeInfo struct {
	Id string `json:"id"`
}

func NewNodePool(rc *redis.Client) NodePool {
	return &nodePoolImpl{
		client:  &fasthttp.Client{MaxConnsPerHost: 1024},
		rootKey: "/",
		counter: counter(),
		root:    nil,
		rc:      rc,
	}
}

func (p *nodePoolImpl) GetRootId(ctx context.Context) (string, error) {
	if p.root != nil {
		return p.root.Id, nil
	}

	if err := p.findRoot(ctx); err == nil {
		return p.root.Id, nil
	}

	if err := p.createRoot(ctx); err != nil {
		return "", err
	}

	return p.root.Id, nil
}
func (p *nodePoolImpl) GetRootKey() string {
	return p.rootKey
}

func (p *nodePoolImpl) createRoot(ctx context.Context) error {
	root, err := p.AcquireNode(ctx)
	if err != nil {
		return err
	}

	rootMeta := &datanode.Metadata{
		Key:       p.rootKey,
		NextNodes: []*datanode.NextRoute{},
	}
	if err := p.PutMetadata(ctx, root, rootMeta); err != nil {
		return err
	}

	p.root = &NodeInfo{Id: root}

	logger.Infof("datanode id %s registered as root", root)
	return nil
}

func (p *nodePoolImpl) findRoot(ctx context.Context) error {
	ids, err := p.rc.Keys(ctx, datanode.HostKey("*")).Result()
	if err != nil {
		return errors.WithStack(err)
	}

	for _, key := range ids {
		id := datanode.IdFromKey(key)
		if _, err := p.GetMetadata(ctx, id, p.rootKey); err != nil {
			continue
		}
		p.root = &NodeInfo{Id: id}
		return nil
	}

	return errors.New("root node not found")
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
