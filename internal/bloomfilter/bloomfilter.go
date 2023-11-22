package bloomfilter

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type BloomFilter struct {
	bit           []bool
	hashFunctions []func(string) (int, error)
	mu            *sync.RWMutex
}

func createHashFunction(size int) func(string) (int, error) {
	base := uuid.Must(uuid.NewRandom()).String()
	h := sha256.New()
	return func(s string) (int, error) {
		defer h.Reset()
		if _, err := h.Write([]byte(s + base)); err != nil {
			return 0, errors.WithStack(err)
		}
		n, err := binary.ReadUvarint(bytes.NewReader(h.Sum(nil)))
		if err != nil {
			return 0, errors.WithStack(err)
		}

		return int(n) % size, nil
	}
}

func New(size, hash int) *BloomFilter {
	hashFunctions := make([]func(string) (int, error), hash)
	for i := range hashFunctions {
		hashFunctions[i] = createHashFunction(size)
	}
	return &BloomFilter{
		bit:           make([]bool, size),
		hashFunctions: hashFunctions,
		mu:            new(sync.RWMutex),
	}
}

func (b *BloomFilter) Get(key string) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, f := range b.hashFunctions {
		n, err := f(key)
		if err != nil {
			return false, err
		}
		if !b.bit[n] {
			return false, nil
		}
	}

	return true, nil
}

func (b *BloomFilter) Set(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, f := range b.hashFunctions {
		n, err := f(key)
		if err != nil {
			return err
		}

		b.bit[n] = true
	}
	return nil
}
