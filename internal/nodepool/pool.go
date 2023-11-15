package nodepool

import (
	"context"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

type NodePool struct {
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

func NewNodePool(redisHost string, redisDB int) *NodePool {
	return &NodePool{
		client:  &fasthttp.Client{MaxConnsPerHost: 1024},
		rootKey: "/",
		counter: counter(),
		root:    nil,
		rc:      redis.NewClient(&redis.Options{Addr: redisHost, DB: redisDB}),
	}
}

func (p *NodePool) GetRootId() string {
	return p.root.Id
}
func (p *NodePool) GetRootKey() string {
	return p.rootKey
}

func (p *NodePool) GetRootMetadata(ctx context.Context) (*datanode.Metadata, error) {
	if p.root != nil {
		return p.GetMetadata(ctx, p.root.Id, p.rootKey)
	}

	if err := p.findRoot(ctx); err == nil {
		return p.GetMetadata(ctx, p.root.Id, p.rootKey)
	}

	if err := p.createRoot(ctx); err != nil {
		return nil, err
	}

	return p.GetMetadata(ctx, p.root.Id, p.rootKey)
}

func (p *NodePool) createRoot(ctx context.Context) error {
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

func (p *NodePool) findRoot(ctx context.Context) error {
	ids, err := p.rc.Keys(ctx, datanode.HostKey("*")).Result()
	if err != nil {
		return errors.WithStack(err)
	}

	for _, id := range ids {
		if _, err := p.GetMetadata(ctx, id, p.rootKey); err != nil {
			continue
		}
		p.root = &NodeInfo{Id: id}
		return nil
	}

	return errors.New("root node not found")
}
