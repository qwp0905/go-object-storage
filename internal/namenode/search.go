package namenode

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func (n *NameNodeImpl) get(ctx context.Context, key, id, current string) (*datanode.Metadata, error) {
	locker := n.lockerPool.Get(current)
	if err := locker.RLock(ctx); err != nil {
		return nil, err
	}
	defer locker.RUnlock(ctx)

	metadata, err := n.pool.GetMetadata(ctx, id, current)
	if err != nil {
		return nil, err
	}

	if key == metadata.Key && metadata.FileExists() {
		return metadata, nil
	}

	for _, next := range metadata.NextNodes {
		if !strings.HasPrefix(key, next.Key) {
			continue
		}
		return n.get(ctx, key, next.NodeId, next.Key)
	}

	return nil, fiber.ErrNotFound
}

// TODO 개선 필요
func (n *NameNodeImpl) scan(
	ctx context.Context,
	prefix, delimiter, after string,
	limit int,
	id, current string,
) (set, []*datanode.Metadata, error) {
	prefixes := set{}
	list := make([]*datanode.Metadata, 0)
	if limit <= 0 {
		return prefixes, list, nil
	}

	locker := n.lockerPool.Get(current)
	if err := locker.RLock(ctx); err != nil {
		return nil, nil, err
	}
	defer locker.RUnlock(ctx)

	metadata, err := n.pool.GetMetadata(ctx, id, current)
	if err != nil {
		return nil, nil, err
	}

	rt := fmt.Sprintf("^%s", prefix)
	if delimiter != "" {
		rt += fmt.Sprintf("[^%s]*", delimiter)
	}
	if metadata.Key > after {
		if matched := regexp.MustCompile(rt).FindString(metadata.Key); matched != "" {
			if metadata.FileExists() && (delimiter == "" || matched == metadata.Key) {
				list = append(list, metadata)
			}
			if delimiter != "" && regexp.MustCompile(rt+delimiter).MatchString(metadata.Key) {
				prefixes.add(matched + delimiter)
			}
		}
	}

	for _, next := range metadata.NextNodes {
		if !(strings.HasPrefix(prefix, next.Key) || strings.HasPrefix(next.Key, prefix)) {
			continue
		}

		s, l, err := n.scan(ctx, prefix, delimiter, after, limit-len(list), next.NodeId, next.Key)
		if err != nil {
			return nil, nil, err
		}
		prefixes.union(s)
		for _, v := range l {
			if len(list) == limit {
				return prefixes, list, nil
			}
			list = append(list, v)
		}
	}

	return prefixes, list, nil
}

type set map[string]struct{}

func (s set) add(key string) {
	s[key] = struct{}{}
}

func (s set) union(u set) {
	for k := range u {
		s.add(k)
	}
}
