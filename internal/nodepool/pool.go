package nodepool

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/valyala/fasthttp"
)

type NodePool struct {
	noCopy   nocopy.NoCopy
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

func (p *NodePool) getNodeHost(id string) string {
	return p.nodeInfo[id].Host
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

func (p *NodePool) getRootMetadata() (*datanode.Metadata, error) {
	if p.root != nil {
		return p.getMetadata(p.root.Host, p.rootKey)
	}

	if err := p.findRoot(); err == nil {
		return p.getMetadata(p.root.Host, p.rootKey)
	}

	if err := p.createRoot(); err != nil {
		return nil, err
	}

	return p.getMetadata(p.root.Host, p.rootKey)
}

func (p *NodePool) createRoot() error {
	root := p.getNodeToSave()
	rootMeta := &datanode.Metadata{
		Key:       p.rootKey,
		NextNodes: []*datanode.NextRoute{},
	}

	if err := p.putMetadata(root.Host, rootMeta); err != nil {
		return err
	}
	p.root = root

	return nil
}

func (p *NodePool) findRoot() error {
	for _, v := range p.nodeInfo {
		if _, err := p.getMetadata(v.Host, p.rootKey); err != nil {
			continue
		}
		p.root = &NodeInfo{Host: v.Host, Id: v.Id}
		return nil
	}

	return errors.New("root node not found")
}
