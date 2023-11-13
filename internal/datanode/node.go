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
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v2"
)

type DataNode struct {
	noCopy nocopy.NoCopy
	bp     *bufferpool.BufferPool
	config *config
	rc     *redis.Client
}

type config struct {
	Id        string `yaml:"id"`
	Host      string `yaml:"host"`
	BaseDir   string `yaml:"base_dir"`
	RedisHost string `yaml:"redis_host"`
	RedisDB   int    `yaml:"redis_db"`
	Port      uint   `yaml:"port"`
}

func (c *config) setDefault() {
	if c.Host == "" {
		c.Host = ""
	}
	if c.Id == "" {
		c.Id = generateKey()
	}
	if c.BaseDir == "" {
		c.BaseDir = "/data"
	}
	if c.RedisDB == 0 {
		c.RedisDB = 1
	}
	if c.RedisHost == "" {
		c.RedisHost = "localhost:6379"
	}
}

func NewDataNode(path string, bp *bufferpool.BufferPool) (*DataNode, uint, error) {
	cfg, err := ensureConfig(path)
	if err != nil {
		return nil, 0, err
	}

	rc := redis.NewClient(&redis.Options{Addr: cfg.RedisHost, DB: cfg.RedisDB})
	if err := rc.SetEx(
		context.Background(),
		cfg.Id,
		fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		time.Hour,
	).Err(); err != nil {
		return nil, 0, errors.WithStack(err)
	}

	if err := ensureDirs(cfg.BaseDir); err != nil {
		return nil, 0, err
	}

	return &DataNode{
		bp:     bp,
		config: cfg,
		rc:     rc,
	}, cfg.Port, nil
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
	if err := filesystem.EnsureDir(fmt.Sprintf("%s/meta/", base)); err != nil {
		return err
	}
	if err := filesystem.EnsureDir(fmt.Sprintf("%s/object/", base)); err != nil {
		return err
	}
	return nil
}

func ensureConfig(path string) (*config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer f.Close()

	cfg := new(config)
	if err := yaml.NewDecoder(f).Decode(cfg); err != nil {
		return nil, errors.WithStack(err)
	}

	cfg.setDefault()

	b, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	u, err := os.Create(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer u.Close()

	if _, err := u.Write(b); err != nil {
		return nil, errors.WithStack(err)
	}

	return cfg, nil
}
