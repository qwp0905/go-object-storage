package nodepool

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/metadata"
	"github.com/valyala/fasthttp"
)

func counter() func(int) int {
	i := 0
	mu := new(sync.Mutex)
	return func(max int) int {
		mu.Lock()
		defer mu.Unlock()
		if i == max {
			i = 0
		}
		return i
	}
}

func (p *nodePoolImpl) PutDirect(ctx context.Context, meta *metadata.Metadata, r io.Reader) error {
	host, err := p.GetNodeHost(ctx, meta.NodeId)
	if err != nil {
		return err
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodPut)
	req.SetRequestURI(getDataHost(host, meta.Source))
	req.SetBodyStream(r, int(meta.Size))

	if err := p.client.Do(req, res); err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode() == fiber.StatusNotFound {
		return fiber.ErrNotFound
	} else if res.StatusCode() >= 400 {
		return errors.WithStack(errors.Errorf("%s", string(res.Body())))
	}

	return nil
}

func (p *nodePoolImpl) GetDirect(ctx context.Context, metadata *metadata.Metadata) (io.Reader, error) {
	host, err := p.GetNodeHost(ctx, metadata.NodeId)
	if err != nil {
		return nil, err
	}

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(getDataHost(host, metadata.Source))
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return nil, errors.WithStack(err)
	}
	go release(ctx, res)
	if res.StatusCode() == fiber.StatusNotFound {
		return nil, fiber.ErrNotFound
	} else if res.StatusCode() >= 400 {
		return nil, errors.WithStack(errors.Errorf("%s", string(res.Body())))
	}

	return res.BodyStream(), nil
}

func (p *nodePoolImpl) DeleteDirect(ctx context.Context, metadata *metadata.Metadata) error {
	host, err := p.GetNodeHost(ctx, metadata.NodeId)
	if err != nil {
		return err
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodDelete)
	req.SetRequestURI(getDataHost(host, metadata.Source))

	if err := p.client.Do(req, res); err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode() == fiber.StatusNotFound {
		return fiber.ErrNotFound
	} else if res.StatusCode() >= 400 {
		return errors.WithStack(errors.Errorf("%s", string(res.Body())))
	}

	return nil
}

func release(ctx context.Context, res *fasthttp.Response) {
	defer fasthttp.ReleaseResponse(res)
	<-ctx.Done()
}

func getDataHost(host, source string) string {
	return fmt.Sprintf("http://%s/data/%s", host, source)
}

func getMetaHost(host, key string) string {
	return fmt.Sprintf("http://%s/meta%s", host, key)
}
