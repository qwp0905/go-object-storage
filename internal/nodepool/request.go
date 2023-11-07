package nodepool

import (
	"encoding/json"
	"fmt"
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

func (p *NodePool) putMetadata(host, key string, metadata *datanode.Metadata) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodPut)
	req.SetRequestURI(fmt.Sprintf("http://%s/meta/%s", host, key))

	if err := json.NewEncoder(req.BodyWriter()).Encode(metadata); err != nil {
		return err
	}

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

	req.Header.SetMethod(fasthttp.MethodHead)
	req.SetRequestURI(fmt.Sprintf("http://%s/meta/%s", host, key))
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return nil, err
	}

	return &res.Header, nil
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
