package service

import (
	"io"

	"github.com/qwp0905/go-object-storage/internal/nodepool"
)

type CRUDService struct {
	pool *nodepool.NodePool
}

func NewCRUDService(pool *nodepool.NodePool) *CRUDService {
	return &CRUDService{pool: pool}
}

func (s *CRUDService) GetObject(key string) (io.Reader, error) {
	metadata, err := s.pool.GetNodeKey(key)
	if err != nil {
		return nil, err
	}

	r, err := s.pool.GetDirect(metadata)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (s *CRUDService) PutObject(key string, r io.Reader) error {
	return nil
}

func (s *CRUDService) DeleteObject(key string) error {
	return nil
}
