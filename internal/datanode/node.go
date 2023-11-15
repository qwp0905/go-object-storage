package datanode

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/bufferpool"
	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/redis/go-redis/v9"
)

type DataNode struct {
	noCopy nocopy.NoCopy
	bp     *bufferpool.BufferPool
	config *Config
	rc     *redis.Client
	id     string
}

type Config struct {
	Host      string
	BaseDir   string
	RedisHost string
	RedisDB   int
}

func NewDataNode(cfg *Config, bp *bufferpool.BufferPool) (*DataNode, error) {
	id, err := ensureId(cfg.BaseDir)
	if err != nil {
		return nil, err
	}

	if err := ensureDirs(cfg.BaseDir); err != nil {
		return nil, err
	}

	return &DataNode{
		bp:     bp,
		config: cfg,
		rc:     redis.NewClient(&redis.Options{Addr: cfg.RedisHost, DB: cfg.RedisDB}),
		id:     id,
	}, nil
}

func (d *DataNode) getMetaKey(key string) string {
	return fmt.Sprintf(
		"%s/meta/%s",
		d.config.BaseDir,
		base64.StdEncoding.EncodeToString([]byte(key)),
	)
}

func (d *DataNode) getDataKey(key string) string {
	return fmt.Sprintf("%s/object/%s", d.config.BaseDir, key)
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

	return id, nil
}

func (n *DataNode) Live() {
	for {
		time.Sleep(time.Second * 30)
		if err := n.rc.SetEx(context.Background(), n.id, n.config.Host, time.Hour).Err(); err != nil {
			logger.Warnf("%+v", errors.WithStack(err))
		}
	}
}
