package nodepool

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

type PoolManager interface {
	Start(sec int)
}

type PoolManagerImpl struct {
	noCopy nocopy.NoCopy
	rc     *redis.Client
	http   *fasthttp.Client
}

func NewPoolManager(rc *redis.Client) PoolManager {
	return &PoolManagerImpl{rc: rc, http: &fasthttp.Client{}}
}

func (m *PoolManagerImpl) Start(sec int) {
	ctx := context.Background()
	timer := time.NewTicker(time.Second * time.Duration(sec))
	for range timer.C {
		nodes, err := m.getAllNodes(ctx)
		if err != nil {
			logger.Errorf("%+v", err)
			continue
		}
		for _, id := range nodes {
			if err := m.healthCheck(ctx, id); err == nil {
				continue
			}
			logger.Errorf("%+v", err)
			if err := m.setNodeDown(ctx, id); err != nil {
				logger.Errorf("%+v", err)
				continue
			}
		}
	}
}

func (m *PoolManagerImpl) getAllNodes(ctx context.Context) ([]string, error) {
	keys, err := m.rc.Keys(ctx, datanode.HostKey("*")).Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := make([]string, 0)
	for _, key := range keys {
		out = append(out, datanode.IdFromKey(key))
	}

	return out, nil
}

func (n *PoolManagerImpl) setNodeDown(ctx context.Context, id string) error {
	return errors.WithStack(n.rc.Del(ctx, datanode.HostKey(id)).Err())
}

func (n *PoolManagerImpl) healthCheck(ctx context.Context, id string) error {
	host, err := n.rc.Get(ctx, datanode.HostKey(id)).Result()
	if err != nil {
		return errors.WithStack(err)
	}
	if err != nil {
		return err
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(fmt.Sprintf("http://%s/health", host))
	if err := n.http.Do(req, res); err != nil {
		return err
	}
	if res.StatusCode() != fasthttp.StatusOK {
		return errors.Errorf("%s", string(res.Body()))
	}

	return nil
}
