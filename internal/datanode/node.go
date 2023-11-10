package datanode

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/valyala/fasthttp"
	"gopkg.in/yaml.v2"
)

type DataNode struct {
	fs   *filesystem.FileSystem
	base string
	id   string
}

type config struct {
	Id         string `yaml:"id" json:"id"`
	Host       string `yaml:"host" json:"host"`
	BaseDir    string `yaml:"base_dir"`
	NameServer string `yaml:"name_server"`
	Port       uint   `yaml:"port" json:"port"`
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
}

func NewDataNode(ctx context.Context, path string) (*DataNode, error) {
	fs := filesystem.NewFileSystem()
	f, err := fs.ReadFile(ctx, path)
	if err != nil {
		return nil, err
	}

	cfg := new(config)
	if err := yaml.NewDecoder(f).Decode(cfg); err != nil {
		return nil, err
	}

	cfg.setDefault()

	b, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	if _, err := fs.WriteFile(path, bytes.NewBuffer(b)); err != nil {
		return nil, err
	}

	for i := 0; i < 5; i++ {
		err = register(cfg)
		if err != nil {
			return &DataNode{id: cfg.Id, base: cfg.BaseDir, fs: fs}, nil
		}
		logger.Warnf("%+v", err)
	}

	return nil, err
}

func (d *DataNode) getMetaKey(key string) string {
	return fmt.Sprintf(
		"%s/meta/%s",
		d.base,
		base64.StdEncoding.EncodeToString([]byte(key)),
	)
}

func (d *DataNode) getDataKey(key string) string {
	return fmt.Sprintf("%s/object/%s", d.base, key)
}

func generateKey() string {
	id, _ := uuid.NewRandom()
	return id.String()
}

func register(cfg *config) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetRequestURI(fmt.Sprintf("http://%s/node/register", cfg.NameServer))

	b, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	req.SetBody(b)

	if err := fasthttp.Do(req, res); err != nil {
		return err
	}

	if res.StatusCode() != fasthttp.StatusOK {
		return errors.Errorf("%s", string(res.Body()))
	}

	return nil
}
