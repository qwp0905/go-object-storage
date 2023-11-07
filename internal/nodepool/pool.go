package nodepool

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"

	"fmt"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/valyala/fasthttp"
)

type NodePool struct {
	client   *fasthttp.Client
	nodeInfo map[string]*NodeInfo
	root     *NodeInfo
	rootKey  string
	locker   *sync.Mutex
	counter  func(int) int
}

type NodeInfo struct {
	Id   string `json:"id"`
	Host string `json:"host"`
}

func (p *NodePool) getNode(id string) *NodeInfo {
	return p.nodeInfo[id]
}

func (p *NodePool) getNodeToSave() *NodeInfo {
	i := 0
	c := p.counter(len(p.nodeInfo))
	for _, v := range p.nodeInfo {
		if c == i {
			return v
		}
		i++
	}
	return nil
}

func NewNodePool(key string) *NodePool {
	return &NodePool{
		client:   &fasthttp.Client{},
		nodeInfo: make(map[string]*NodeInfo),
		rootKey:  key,
		locker:   new(sync.Mutex),
		counter:  counter(),
	}
}

func (p *NodePool) GetDirect(metadata *datanode.Metadata) (io.Reader, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(fmt.Sprintf(
		"http://%s/data/%s",
		p.getNode(metadata.NodeId).Host,
		metadata.Source,
	))
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return nil, err
	}

	return res.BodyStream(), nil
}

func (p *NodePool) SearchMetadata(key string) (*datanode.Metadata, error) {
	root, err := p.getMetadata(p.root.Host, p.rootKey)
	if err != nil {
		return nil, err
	}
	return p.search(key, root)
}

func (p *NodePool) PutDirect(r io.Reader) (*datanode.Metadata, *datanode.NextRoute, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	node := p.getNodeToSave()
	req.Header.SetMethod(fasthttp.MethodPut)
	req.SetRequestURI(fmt.Sprintf("http://%s/data/", node.Host))

	if err := p.client.Do(req, res); err != nil {
		return nil, nil, err
	}

	if res.StatusCode() >= 300 {
		return nil, nil, errors.Errorf("%s", string(res.Body()))
	}

	data := new(datanode.Metadata)
	if err := json.NewDecoder(bytes.NewBuffer(res.Body())).Decode(data); err != nil {
		return nil, nil, err
	}

	return data, &datanode.NextRoute{NodeId: node.Id}, nil
}
