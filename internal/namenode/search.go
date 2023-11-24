package namenode

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/metadata"
	"github.com/qwp0905/go-object-storage/pkg/list"
)

func (n *nameNodeImpl) get(ctx context.Context, key, id, current string) (*metadata.Metadata, error) {
	locker := n.lockerPool.Get(current)
	if err := locker.RLock(ctx); err != nil {
		return nil, err
	}

	currentMeta, err := n.pool.GetMetadata(ctx, id, current)
	if err != nil {
		defer locker.RUnlock(ctx)
		return nil, err
	}

	if key == currentMeta.Key && currentMeta.FileExists() {
		defer locker.RUnlock(ctx)
		return currentMeta, nil
	}

	if index := currentMeta.FindPrefix(key); index != -1 {
		next := currentMeta.GetNext(index)
		if err := locker.RUnlock(ctx); err != nil {
			return nil, err
		}

		return n.get(ctx, key, next.NodeId, next.Key)
	}

	defer locker.RUnlock(ctx)
	return nil, fiber.ErrNotFound
}

// TODO 쬐끔 개선된 듯...
func (n *nameNodeImpl) scan(
	ctx context.Context,
	prefix, delimiter, after string,
	limit int,
	id, current string,
) (list.Set[string], []*metadata.Metadata, error) {
	prefixes := make(list.Set[string])
	list := make([]*metadata.Metadata, 0)
	if limit <= 0 {
		return prefixes, list, nil
	}

	reg := fmt.Sprintf("^%s", prefix)
	if delimiter != "" {
		reg += fmt.Sprintf("[^%s]*", delimiter)
		if dir := regexp.MustCompile(reg + delimiter).FindString(current); dir != "" {
			if after < current {
				prefixes.Add(dir)
			}
			return prefixes, list, nil
		}
	} else {
		reg += ".*"
	}

	locker := n.lockerPool.Get(current)
	if err := locker.RLock(ctx); err != nil {
		return nil, nil, err
	}
	defer locker.RUnlock(ctx)

	currentMeta, err := n.pool.GetMetadata(ctx, id, current)
	if err != nil {
		return nil, nil, err
	}

	matched := regexp.MustCompile(reg).FindString(current)
	if matched != "" && matched == current && currentMeta.FileExists() && current > after {
		list = append(list, currentMeta)
	}

	for _, next := range currentMeta.NextNodes {
		if strings.HasPrefix(prefix, next.Key) {
			return n.scan(ctx, prefix, delimiter, after, limit, next.NodeId, next.Key)
		}

		if !strings.HasPrefix(next.Key, prefix) {
			continue
		}

		if !strings.HasPrefix(after, next.Key) && after > next.Key {
			continue
		}

		p, l, err := n.scan(ctx, prefix, delimiter, after, limit-len(list), next.NodeId, next.Key)
		if err != nil {
			return nil, nil, err
		}
		prefixes.Union(p)
		for _, v := range l {
			if len(list) == limit {
				return prefixes, list, nil
			}
			list = append(list, v)
		}
	}

	return prefixes, list, nil
}
