package datanode

type DataNode struct{}

type Metadata struct {
	Key       string   `json:"key"`
	Source    string   `json:"source"`
	Size      string   `json:"size"`
	Node      string   `json:"node"`
	NextNodes []string `json:"next_nodes"`
}

type NextData struct {
	Key    string `json:"key"`
	Source string `json:"source"`
	Node   string `json:"node"`
}
