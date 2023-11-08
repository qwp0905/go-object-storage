package nodepool

import (
	"sync"

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

func (p *NodePool) getRootMetadata() (*datanode.Metadata, error) {
	return p.getMetadata(p.root.Host, p.rootKey)
}
