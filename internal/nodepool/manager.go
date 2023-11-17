package nodepool

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

type PoolManager struct {
	noCopy nocopy.NoCopy
	rc     *redis.Client
	http   *fasthttp.Client
}

func NewPoolManager(rc *redis.Client) *PoolManager {
	return &PoolManager{rc: rc, http: &fasthttp.Client{}}
}

func (m *PoolManager) GetAllNodes(ctx context.Context) ([]string, error) {
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

func (n *PoolManager) SetNodeDown(ctx context.Context, id string) error {
	return errors.WithStack(n.rc.Del(ctx, datanode.HostKey(id)).Err())
}

func (n *PoolManager) HealthCheck(ctx context.Context, id string) error {
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
