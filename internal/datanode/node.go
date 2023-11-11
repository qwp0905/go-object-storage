package datanode

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/internal/bufferpool"
	"github.com/qwp0905/go-object-storage/internal/filesystem"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/qwp0905/go-object-storage/pkg/nocopy"
	"github.com/valyala/fasthttp"
	"gopkg.in/yaml.v2"
)

type DataNode struct {
	noCopy nocopy.NoCopy
	bp     *bufferpool.BufferPool
	base   string
	id     string
	config *config
}

type config struct {
	Id         string `yaml:"id"`
	Host       string `yaml:"host"`
	BaseDir    string `yaml:"base_dir"`
	NameServer string `yaml:"nameserver"`
	Port       uint   `yaml:"port"`
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

func NewDataNode(path string, bp *bufferpool.BufferPool) (*DataNode, uint, error) {
	cfg, err := ensureConfig(path)
	if err != nil {
		return nil, 0, err
	}

	if err := ensureDirs(cfg.BaseDir); err != nil {
		return nil, 0, err
	}

	return &DataNode{
		id:     cfg.Id,
		base:   cfg.BaseDir,
		bp:     bp,
		config: cfg,
	}, cfg.Port, nil
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

func (d *DataNode) Register() error {
	var err error
	for i := 0; i < 5; i++ {
		time.Sleep(time.Second)
		err = register(d.config)
		if err == nil {
			logger.Info("datanode registered")
			return nil
		}
		logger.Warnf("%+v", err)
	}
	panic(err)
}

type registerBody struct {
	Id   string `json:"id"`
	Host string `json:"host"`
	Port uint   `json:"port"`
}

func register(cfg *config) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetRequestURI(fmt.Sprintf("http://%s/node/register", cfg.NameServer))
	req.Header.SetContentType("application/json")

	b, err := json.Marshal(&registerBody{
		Id:   cfg.Id,
		Host: cfg.Host,
		Port: cfg.Port,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	req.SetBody(b)

	if err := fasthttp.Do(req, res); err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode() != fasthttp.StatusOK {
		return errors.Errorf("%s", string(res.Body()))
	}

	return nil
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
