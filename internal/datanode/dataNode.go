package datanode

type DataNode struct{}

type Metadata struct {
	Key       string      `json:"key"`
	Source    string      `json:"source"`
	Size      string      `json:"size"`
	NodeId    string      `json:"node_id"`
	NextNodes []*Metadata `json:"next_nodes"`
}
