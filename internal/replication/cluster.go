package replication

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/pkg/errors"
	"github.com/qwp0905/go-object-storage/pkg/logger"
	"github.com/valyala/fasthttp"
)

const (
	stateLeader    = "leader"
	stateFollower  = "follower"
	stateCandidate = "candidate"
)

type Cluster interface {
	StartReplication()
	Voting(state *VoteState) error
	Heartbeat()
	Quorum() int
}

type Heartbeat struct {
	Term uint64 `json:"term"`
	Log  *Log   `json:"log,omitempty"`
}

type VoteState struct {
	Term      uint64 `json:"term"`
	LastIndex uint64 `json:"last_index"`
}

type clusterImpl struct {
	log       *logStoreImpl
	term      uint64
	heartbeat chan struct{}
	state     string
	hosts     []string
}

func NewCluster() Cluster {
	return &clusterImpl{}
}

func (c *clusterImpl) Heartbeat() {
	c.heartbeat <- struct{}{}
}

func (c *clusterImpl) Quorum() int {
	return len(c.hosts)/2 + 1
}

func (c *clusterImpl) Voting(state *VoteState) error {
	if state.Term <= c.term || state.LastIndex <= c.log.LastIndex() {
		return errors.WithStack(fiber.ErrBadRequest)
	}

	return nil
}

func (c *clusterImpl) StartReplication() {
	for {
		select {
		case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
			if err := c.upgrade(); err != nil {
				logger.Warnf("%+v", err)
				continue
			}
			return
		case <-c.heartbeat:
			continue
		}
	}
}

func (c *clusterImpl) upgrade() error {
	c.state = stateCandidate
	c.term += 1

	wg := new(sync.WaitGroup)
	done := make(chan struct{})
	wg.Add(len(c.hosts))
	go func() {
		defer close(done)
		defer wg.Wait()
		for _, v := range c.hosts {
			go func(host string) {
				defer wg.Done()
				if err := c.requestVote(host); err != nil {
					logger.Warnf("%+v", err)
					return
				}
				done <- struct{}{}
			}(v)
		}
	}()

	allowed := 1
	for range done {
		if allowed = allowed + 1; allowed < c.Quorum() {
			continue
		}
		c.state = stateLeader
		go c.sendHeartbeat()
		return nil
	}

	return errors.New("election failed")
}

func (c *clusterImpl) requestVote(host string) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetRequestURI(fmt.Sprintf("http://%s/vote", host))

	if err := json.NewEncoder(req.BodyWriter()).Encode(&VoteState{
		Term:      c.term,
		LastIndex: c.log.LastIndex(),
	}); err != nil {
		return errors.WithStack(err)
	}

	if err := fasthttp.Do(req, res); err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode() >= 300 {
		return errors.New(string(res.Body()))
	}

	return nil
}

func (c *clusterImpl) sendHeartbeat() {
	t := time.NewTicker(time.Millisecond * 100)
	for range t.C {
		if err := c.replicateLogs(); err != nil {
			logger.Warnf("%+v", err)
			continue
		}
	}
}

func (c *clusterImpl) replicateLogs() error {
	logs := c.log.Uncommitted()
	done := make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(len(c.hosts))

	go func() {
		defer close(done)
		defer wg.Wait()
		for _, v := range c.hosts {
			go func(host string) {
				if err := c.sendLogs(host, logs); err != nil {
					logger.Warnf("%+v", err)
					return
				}
				done <- struct{}{}
			}(v)
		}
	}()

	allowed := 1
	for range done {
		if allowed = allowed + 1; allowed < c.Quorum() {
			continue
		}
		return c.log.Commit(c.log.LastIndex())
	}
	return nil
}

func (c *clusterImpl) sendLogs(host string, logs []*Log) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)

	req.SetRequestURI(fmt.Sprintf("http://%s/repl/log", host))
	req.Header.SetMethod(fasthttp.MethodPut)

	if err := json.NewEncoder(req.BodyWriter()).Encode(logs); err != nil {
		return errors.WithStack(err)
	}

	if err := fasthttp.Do(req, res); err != nil {
		return errors.WithStack(err)
	}

	if err := fasthttp.Do(req, res); err != nil {
		return errors.WithStack(err)
	}

	if res.StatusCode() >= 300 {
		return errors.New(string(res.Body()))
	}

	return nil
}
