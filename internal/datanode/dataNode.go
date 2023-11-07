package datanode

type DataNode struct{}

type Metadata struct {
	Key       string       `json:"key"`
	Source    string       `json:"source"`
	Size      string       `json:"size"`
	NodeId    string       `json:"node_id"`
	NextNodes []*NextRoute `json:"next_nodes"`
}

type NextRoute struct {
	NodeId string `json:"node_id"`
	Key    string `json:"key"`
}

func (m *Metadata) FileExists() bool {
	return m.Source != ""
}
