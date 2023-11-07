package nodepool

import (
	"encoding/json"
	"io"

	"fmt"

	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/valyala/fasthttp"
)

type NodePool struct {
	client   *fasthttp.Client
	nodeInfo map[string]*nodeInfo
	root     *datanode.Metadata
	rootKey  string
}

type nodeInfo struct {
	*NodeInfo
	host string
}

type NodeInfo struct {
	Id string `json:"id"`
}

func (p *NodePool) getNode(id string) *nodeInfo {
	return p.nodeInfo[id]
}

func NewNodePool(hosts ...string) (*NodePool, error) {
	pool := &NodePool{client: &fasthttp.Client{}, nodeInfo: make(map[string]*nodeInfo)}
	// locker := new(sync.Mutex)
	// wg := new(sync.WaitGroup)
	// ech := make(chan error)
	// go func() {
	// 	defer close(ech)
	// 	defer wg.Wait()
	// 	for _, host := range hosts {
	// 		wg.Add(1)
	// 		go func(h string) {
	// 			defer wg.Done()
	// 			req := fasthttp.AcquireRequest()
	// 			defer fasthttp.ReleaseRequest(req)
	// 			res := fasthttp.AcquireResponse()
	// 			defer fasthttp.ReleaseResponse(res)
	// 			req.Header.SetMethod(fasthttp.MethodGet)
	// 			req.SetRequestURI(fmt.Sprintf("http://%s/info", h))
	// 			res.StreamBody = true

	// 			if err := pool.client.Do(req, res); err != nil {
	// 				logger.Errorf("%+v", errors.WithStack(err))
	// 				ech <- errors.Errorf("unable to connect node %s...", h)
	// 				return
	// 			}
	// 			info := new(NodeInfo)
	// 			if err := json.NewDecoder(res.BodyStream()).Decode(info); err != nil {
	// 				ech <- errors.WithStack(err)
	// 			}
	// 			locker.Lock()
	// 			defer locker.Unlock()
	// 			if info.Root {
	// 				if pool.root != nil {
	// 					ech <- errors.Errorf("root node already exists but receive %s...", h)
	// 					return
	// 				}
	// 				pool.root = &nodeInfo{NodeInfo: info, host: h}
	// 			} else {
	// 				pool.nodeInfo[info.Id] = &nodeInfo{NodeInfo: info, host: h}
	// 			}
	// 		}(host)
	// 	}
	// }()

	// if err := <-ech; err != nil {
	// 	return nil, err
	// }
	if pool.root != nil {
		return nil, errors.New("root node doesn't exists...")
	}

	return pool, nil
}

func (p *NodePool) GetDirect(metadata *datanode.Metadata) (io.Reader, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(fmt.Sprintf(
		"http://%s/data/%s",
		p.getNode(metadata.NodeId).host,
		metadata.Source,
	))
	res.StreamBody = true

	if err := p.client.Do(req, res); err != nil {
		return nil, err
	}

	return res.BodyStream(), nil
}

func (p *NodePool) SearchMetadata(key string) (*datanode.Metadata, error) {
	return p.search(key, p.root)
}

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
