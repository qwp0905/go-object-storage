package nodepool

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/valyala/fasthttp"
)

func (p *NodePool) GetNodeHost(ctx context.Context, id string) (string, error) {
	host, err := p.rc.Get(ctx, id).Result()
	if err != nil {
		return "", errors.WithStack(err)
	}

	return host, nil
}

func (p *NodePool) AcquireNode(ctx context.Context) (string, error) {
	ids, err := p.rc.Keys(ctx, "*").Result()
	if err != nil {
		return "", errors.WithStack(err)
	}

	if len(ids) == 0 {
		return "", errors.New("no datanode registered...")
	}

	return ids[p.counter(len(ids))], nil
}

func (p *NodePool) CheckAliveNodes() error {
	for {
		time.Sleep(time.Minute)
		if err := p.checkAround(); err != nil {
			logger.Warnf("%+v", err)
		}
	}
}

func (p *NodePool) checkAround() error {
	ctx := context.Background()
	ids, err := p.rc.Keys(ctx, "*").Result()
	if err != nil {
		return errors.WithStack(err)
	}

	for _, id := range ids {
		if err := p.healthCheck(ctx, id); err != nil {
			logger.Warnf("%+v", err)
			if err := p.rc.Del(ctx, id).Err(); err != nil {
				logger.Warnf("%+v", err)
			}
			continue
		}
	}
	return nil
}

func (p *NodePool) healthCheck(ctx context.Context, id string) error {
	host, err := p.GetNodeHost(ctx, id)
	if err != nil {
		return err
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(fmt.Sprintf("http://%s/health", host))
	if err := p.client.Do(req, res); err != nil {
		return err
	}
	if res.StatusCode() != fasthttp.StatusOK {
		return errors.Errorf("%s", string(res.Body()))
	}

	return nil
}
