package datanode

import "github.com/valyala/fasthttp"

type DataNodes struct {
	nodes  chan *nodeInfo
	client *fasthttp.Client
}

type nodeInfo struct {
	host  string
	retry int
}

type NodeInfo struct {
	Id string `json:"id"`
}

func NewDataNode(hosts ...string) (*DataNodes, error) {
	ch := make(chan *nodeInfo)
	for _, v := range hosts {
		ch <- &nodeInfo{host: v, retry: 0}
	}
	return &DataNodes{nodes: ch, client: &fasthttp.Client{}}, nil
}

func Request() {}
