package service

import (
	"io"

	"github.com/qwp0905/go-object-storage/internal/nodepool"
)

type ApiService struct {
	pool *nodepool.NodePool
}

func NewApiService(pool *nodepool.NodePool) *ApiService {
	return &ApiService{pool: pool}
}

func (s *ApiService) GetObject(key string) (io.Reader, error) {
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

func (s *ApiService) PutObject(key string, r io.Reader) error {
	return nil
}

func (s *ApiService) DeleteObject(key string) error {
	return nil
}
