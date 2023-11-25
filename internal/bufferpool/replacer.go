package bufferpool

func (p *bufferPoolImpl) available() int {
	return p.maxSize - p.table.allocated
}

func (p *bufferPoolImpl) isAllowed(size int) bool {
	return p.maxSize/2 > size
}

func (p *bufferPoolImpl) victim(size int) error {
	if size <= 0 {
		return nil
	}

	page := p.table.oldest()
	if page.isDirty() {
		if _, err := p.fs.WriteFile(page.key, page.getData()); err != nil {
			return err
		}
	}

	s := page.getSize()
	p.table.deallocate(page.key)
	return p.victim(size - s)
}

func (p *bufferPoolImpl) acquire(size int) error {
	return p.victim(size - p.available())
}
