package nodepool

func (p *NodePool) Register(id string, host string) error {
	meta, err := p.getMetadata(host, p.rootKey)
}
