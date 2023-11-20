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
	// defer locker.RUnlock(ctx) //TODO 다르게 처리해보자 지금은 그냥 중첩 락이야...

	currentMeta, err := n.pool.GetMetadata(ctx, id, current)
	if err != nil {
		defer locker.RUnlock(ctx)
		return nil, err
	}

	if key == currentMeta.Key && currentMeta.FileExists() {
		defer locker.RUnlock(ctx)
		return currentMeta, nil
	}

	if next := currentMeta.FindKey(key); next != nil {
		if err := locker.RUnlock(ctx); err != nil {
			return nil, err
		}

		return n.get(ctx, key, next.NodeId, next.Key)
	}

	defer locker.RUnlock(ctx)
	return nil, fiber.ErrNotFound
}

// TODO 개선 필요
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

	locker := n.lockerPool.Get(current)
	if err := locker.RLock(ctx); err != nil {
		return nil, nil, err
	}
	defer locker.RUnlock(ctx)

	currentMeta, err := n.pool.GetMetadata(ctx, id, current)
	if err != nil {
		return nil, nil, err
	}

	rt := fmt.Sprintf("^%s", prefix)
	if delimiter != "" {
		rt += fmt.Sprintf("[^%s]*", delimiter)
	}
	if currentMeta.Key > after {
		if matched := regexp.MustCompile(rt).FindString(currentMeta.Key); matched != "" {
			if currentMeta.FileExists() && (delimiter == "" || matched == currentMeta.Key) {
				list = append(list, currentMeta)
			}
			if delimiter != "" && regexp.MustCompile(rt+delimiter).MatchString(currentMeta.Key) {
				prefixes.Add(matched + delimiter)
			}
		}
	}

	for _, next := range currentMeta.NextNodes {
		if !(strings.HasPrefix(prefix, next.Key) || strings.HasPrefix(next.Key, prefix)) {
			continue
		}

		s, l, err := n.scan(ctx, prefix, delimiter, after, limit-len(list), next.NodeId, next.Key)
		if err != nil {
			return nil, nil, err
		}
		prefixes.Union(s)
		for _, v := range l {
			if len(list) == limit {
				return prefixes, list, nil
			}
			list = append(list, v)
		}
	}

	return prefixes, list, nil
}
