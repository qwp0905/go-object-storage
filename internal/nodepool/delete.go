package nodepool

import "github.com/pkg/errors"

func (p *NodePool) DeleteObject(key string) error {
	if len(p.nodeInfo) == 0 {
		return errors.New("no host registered...")
	}

	root, err := p.GetRootMetadata()
	if err != nil {
		return err
	}

	id, metadata, err := p.search(p.root.Id, key, root)
	if err != nil {
		return err
	}

	if err := p.DeleteDirect(metadata); err != nil {
		return err
	}

	return p.DeleteMetadata(p.getNodeHost(id), key)
}
