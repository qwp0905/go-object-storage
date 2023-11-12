package nodepool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"

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

func (p *NodePool) PutDirect(metadata *datanode.Metadata, r io.Reader) (*datanode.Metadata, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodPut)
	req.SetRequestURI(p.getDataHost(metadata))
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
	if err := json.NewDecoder(bytes.NewBuffer(res.Body())).Decode(data); err != nil {
		return nil, errors.WithStack(err)
	}

	return data, nil
}

func (p *NodePool) GetDirect(ctx context.Context, metadata *datanode.Metadata) (io.Reader, error) {
	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(p.getDataHost(metadata))
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

func (p *NodePool) DeleteDirect(metadata *datanode.Metadata) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodDelete)
	req.SetRequestURI(p.getDataHost(metadata))

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

func (p *NodePool) getDataHost(metadata *datanode.Metadata) string {
	return fmt.Sprintf("http://%s/data/%s", p.getNodeHost(metadata.NodeId), metadata.Source)
}

func (p *NodePool) getMetaHost(id string, key string) string {
	return fmt.Sprintf("http://%s/meta%s", p.getNodeHost(id), key)
}
