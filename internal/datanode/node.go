package datanode

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/bufferpool"
	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/internal/metadata"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/redis/go-redis/v9"
)

func HostKey(id string) string {
	return fmt.Sprintf("HOST:%s", id)
}

func IdFromKey(key string) string {
	return strings.TrimPrefix(key, "HOST:")
}

type DataNode interface {
	GetMetadata(key string) (*metadata.Metadata, error)
	PutMetadata(metadata *metadata.Metadata) error
	DeleteMetadata(key string) error
	GetObject(ctx context.Context, key string) (io.Reader, error)
	PutObject(key string, size int, r io.Reader) error
	DeleteObject(key string) error
	Live()
}

type dataNodeImpl struct {
	noCopy nocopy.NoCopy
	bp     bufferpool.BufferPool
	config *Config
	rc     *redis.Client
	id     string
}

type Config struct {
	Host      string
	RedisHost string
	RedisDB   int
}

func NewDataNode(basedir string, cfg *Config, bp bufferpool.BufferPool) (DataNode, error) {
	id, err := ensureId(basedir)
	if err != nil {
		return nil, err
	}

	if err := ensureDirs(basedir); err != nil {
		return nil, err
	}

	return &dataNodeImpl{
		bp:     bp,
		config: cfg,
		rc:     redis.NewClient(&redis.Options{Addr: cfg.RedisHost, DB: cfg.RedisDB}),
		id:     id,
	}, nil
}

func (d *dataNodeImpl) getMetaKey(key string) string {
	return fmt.Sprintf(
		"meta/%s",
		base64.StdEncoding.EncodeToString([]byte(key)),
	)
}

func (d *dataNodeImpl) getDataKey(key string) string {
	return fmt.Sprintf("object/%s", key)
}

func generateKey() string {
	id, _ := uuid.NewRandom()
	return id.String()
}

func ensureDirs(base string) error {
	if err := filesystem.EnsureDir(fmt.Sprintf("%s/meta", base)); err != nil {
		return err
	}
	if err := filesystem.EnsureDir(fmt.Sprintf("%s/object", base)); err != nil {
		return err
	}
	return nil
}

func ensureId(base string) (string, error) {
	path := fmt.Sprintf("%s/id", base)

	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return "", errors.WithStack(err)
	}
	if err == nil {
		logger.Infof("start with exists volume id %s", string(b))
		return string(b), nil
	}

	id := generateKey()
	f, err := os.Create(path)
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer f.Close()

	if _, err := f.WriteString(id); err != nil {
		return "", errors.WithStack(err)
	}

	logger.Infof("start with new volume id %s", id)
	return id, nil
}

func (n *dataNodeImpl) Live() {
	if err := n.register(); err != nil {
		logger.Warnf("%+v", errors.WithStack(err))
	}
	for range time.NewTicker(time.Second * 30).C {
		if err := n.register(); err != nil {
			logger.Warnf("%+v", errors.WithStack(err))
		}
	}
}

func (n *dataNodeImpl) register() error {
	return errors.WithStack(n.rc.SetEx(
		context.Background(),
		HostKey(n.id),
		n.config.Host,
		time.Hour,
	).Err())
}
