package nodepool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/valyala/fasthttp"
)

func (p *NodePool) getMetadata(host string, key string) (*datanode.Metadata, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(fmt.Sprintf("http://%s/meta/%s", host, key))
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return nil, err
	}

	data := new(datanode.Metadata)
	if err := json.NewDecoder(res.BodyStream()).Decode(data); err != nil {
		return nil, err
	}

	return data, nil
}

func (p *NodePool) putMetadata(host string, metadata *datanode.Metadata) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodPut)
	req.SetRequestURI(fmt.Sprintf("http://%s/meta/%s", host, metadata.Key))
	b, err := json.Marshal(metadata)
	if err != nil {
		return errors.WithStack(err)
	}
	req.SetBodyStream(bytes.NewReader(b), len(b))

	if err := p.client.Do(req, res); err != nil {
		return err
	}

	if res.StatusCode() >= 300 {
		return errors.Errorf("%s", string(res.Body()))
	}

	return nil
}

func (p *NodePool) headMetadata(host string, key string) (*fasthttp.ResponseHeader, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(fmt.Sprintf("http://%s/meta/%s", host, key))

	if err := p.client.Do(req, res); err != nil {
		return nil, errors.WithStack(err)
	}

	return &res.Header, nil
}

func (p *NodePool) deleteMetadata(host string, key string) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodDelete)
	req.SetRequestURI(fmt.Sprintf("http://%s/meta/%s", host, key))
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode() >= 300 {
		return errors.Errorf("%s", string(res.Body()))
	}

	return nil
}

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

func (p *NodePool) putDirect(size int, r io.Reader) (*datanode.Metadata, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	node := p.getNodeToSave()
	req.Header.SetMethod(fasthttp.MethodPut)
	req.SetRequestURI(fmt.Sprintf("http://%s/data/", node.Host))
	req.SetBodyStream(r, size)

	if err := p.client.Do(req, res); err != nil {
		return nil, errors.WithStack(err)
	}

	if res.StatusCode() >= 300 {
		return nil, errors.Errorf("%s", string(res.Body()))
	}

	data := new(datanode.Metadata)
	if err := json.NewDecoder(bytes.NewBuffer(res.Body())).Decode(data); err != nil {
		return nil, errors.WithStack(err)
	}

	return data, nil
}

func (p *NodePool) getDirect(ctx context.Context, metadata *datanode.Metadata) (io.Reader, error) {
	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(fmt.Sprintf(
		"http://%s/data/%s",
		p.getNodeHost(metadata.NodeId),
		metadata.Source,
	))
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return nil, errors.WithStack(err)
	}
	go release(ctx, res)

	return res.BodyStream(), nil
}

func (p *NodePool) deleteDirect(metadata *datanode.Metadata) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodDelete)
	req.SetRequestURI(fmt.Sprintf(
		"http://%s/data/%s",
		p.getNodeHost(metadata.NodeId),
		metadata.Source,
	))

	if err := p.client.Do(req, res); err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode() >= 300 {
		return errors.Errorf("%s", string(res.Body()))
	}

	return nil
}

func release(ctx context.Context, res *fasthttp.Response) {
	defer fasthttp.ReleaseResponse(res)
	<-ctx.Done()
}
