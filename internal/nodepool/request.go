package nodepool

import (
	"bytes"
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

func (p *NodePool) deleteMetadata(host string, key string) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodDelete)
	req.SetRequestURI(fmt.Sprintf("http://%s/meta/%s", host, key))
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return err
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

func (p *NodePool) putDirect(r io.Reader) (*datanode.Metadata, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	node := p.getNodeToSave()
	req.Header.SetMethod(fasthttp.MethodPut)
	req.SetRequestURI(fmt.Sprintf("http://%s/data/", node.Host))

	if err := p.client.Do(req, res); err != nil {
		return nil, err
	}

	if res.StatusCode() >= 300 {
		return nil, errors.Errorf("%s", string(res.Body()))
	}

	data := new(datanode.Metadata)
	if err := json.NewDecoder(bytes.NewBuffer(res.Body())).Decode(data); err != nil {
		return nil, err
	}

	return data, nil
}

func (p *NodePool) getDirect(metadata *datanode.Metadata) (chan<- struct{}, io.Reader, error) {
	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(fmt.Sprintf(
		"http://%s/data/%s",
		p.getNodeHost(metadata.NodeId),
		metadata.Source,
	))
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return nil, nil, err
	}

	done := make(chan struct{})
	defer wait(done)

	//TODO 문제 가능성 있음 response가 close되었을 때 정상적으로 read가 불가할듯
	return done, res.BodyStream(), nil
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
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return err
	}

	if res.StatusCode() >= 300 {
		return errors.Errorf("%s", string(res.Body()))
	}

	return nil
}

func wait(c chan struct{}) {
	<-c
}
