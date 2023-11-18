package nodepool

import (
	"bytes"
	"context"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/valyala/fasthttp"
)

func (p *NodePool) GetMetadata(ctx context.Context, id, key string) (*datanode.Metadata, error) {
	host, err := p.GetNodeHost(ctx, id)
	if err != nil {
		return nil, err
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(getMetaHost(host, key))
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return nil, err
	}

	if res.StatusCode() == fiber.StatusNotFound {
		return nil, fiber.ErrNotFound
	} else if res.StatusCode() >= 400 {
		return nil, errors.WithStack(errors.Errorf("%s", string(res.Body())))
	}

	data := new(datanode.Metadata)
	if err := json.NewDecoder(res.BodyStream()).Decode(data); err != nil {
		return nil, err
	}

	return data, nil
}

func (p *NodePool) PutMetadata(ctx context.Context, id string, metadata *datanode.Metadata) error {
	host, err := p.GetNodeHost(ctx, id)
	if err != nil {
		return err
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodPut)
	req.SetRequestURI(getMetaHost(host, ""))
	req.Header.SetContentType("application/json")

	b, err := json.Marshal(metadata)
	if err != nil {
		return errors.WithStack(err)
	}
	req.SetBodyStream(bytes.NewReader(b), len(b))

	if err := p.client.Do(req, res); err != nil {
		return err
	}

	if res.StatusCode() == fiber.StatusNotFound {
		return fiber.ErrNotFound
	} else if res.StatusCode() >= 400 {
		return errors.WithStack(errors.Errorf("%s", string(res.Body())))
	}

	return nil
}

func (p *NodePool) DeleteMetadata(ctx context.Context, id, key string) error {
	host, err := p.GetNodeHost(ctx, id)
	if err != nil {
		return err
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodDelete)
	req.SetRequestURI(getMetaHost(host, key))
	res.StreamBody = true

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
