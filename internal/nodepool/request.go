package nodepool

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/valyala/fasthttp"
)

func counter() func(int) int {
	i := int32(0)
	return func(max int) int {
		atomic.AddInt32(&i, 1)
		if i == int32(max) {
			atomic.StoreInt32(&i, 0)
		}
		return int(i)
	}
}

func (p *NodePoolImpl) PutDirect(ctx context.Context, metadata *datanode.Metadata, r io.Reader) (*datanode.Metadata, error) {
	host, err := p.GetNodeHost(ctx, metadata.NodeId)
	if err != nil {
		return nil, err
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodPut)
	req.SetRequestURI(getDataHost(host, metadata.Source))
	req.SetBodyStream(r, int(metadata.Size))

	if err := p.client.Do(req, res); err != nil {
		return nil, errors.WithStack(err)
	}

	if res.StatusCode() == fiber.StatusNotFound {
		return nil, fiber.ErrNotFound
	} else if res.StatusCode() >= 400 {
		return nil, errors.WithStack(errors.Errorf("%s", string(res.Body())))
	}

	data := new(datanode.Metadata)
	if err := json.NewDecoder(bytes.NewReader(res.Body())).Decode(data); err != nil {
		return nil, errors.WithStack(err)
	}

	return data, nil
}

func (p *NodePoolImpl) GetDirect(ctx context.Context, metadata *datanode.Metadata) (io.Reader, error) {
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

func (p *NodePoolImpl) DeleteDirect(ctx context.Context, metadata *datanode.Metadata) error {
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
