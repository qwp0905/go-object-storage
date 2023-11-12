package namenode

import (
	"io"

	"github.com/qwp0905/go-object-storage/internal/datanode"
	"github.com/qwp0905/go-object-storage/internal/nodepool"
)

type NameNode struct {
	pool *nodepool.NodePool
}

func New(pool *nodepool.NodePool) *NameNode {
	return &NameNode{pool: pool}
}

func (n *NameNode) HeadObject(key string) (*datanode.Metadata, error) {

	return nil, nil
}

func (n *NameNode) GetObject(key string) (io.Reader, error) {
	return nil, nil
}

func (n *NameNode) PutObject(key string, size int, r io.Reader) error {
	return nil
}

func (n *NameNode) DeleteObject(key string) error {
	return nil
}
