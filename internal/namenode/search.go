package namenode

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/qwp0905/go-object-storage/internal/datanode"
)

func (n *NameNode) get(ctx context.Context, id, key string, metadata *datanode.Metadata) (string, *datanode.Metadata, error) {
	if key == metadata.Key && metadata.FileExists() {
		return id, metadata, nil
	}
	for _, next := range metadata.NextNodes {
		if !strings.HasPrefix(key, next.Key) {
			continue
		}

		nextMeta, err := n.pool.GetMetadata(ctx, next.NodeId, next.Key)
		if err != nil {
			return "", nil, err
		}
		return n.get(ctx, next.NodeId, key, nextMeta)
	}

	return "", nil, fiber.ErrNotFound
}

func (n *NameNode) scan(
	ctx context.Context,
	prefix string,
	delimiter string,
	limit int,
	metadata *datanode.Metadata,
) (set, []*datanode.Metadata, error) {
	prefixes := set{}
	list := make([]*datanode.Metadata, 0)
	rt := fmt.Sprintf("^%s", prefix)
	if delimiter != "" {
		rt += fmt.Sprintf("[^%s]*", delimiter)
	}
	if matched := regexp.MustCompile(rt).FindString(metadata.Key); matched != "" {
		if matched == metadata.Key && metadata.FileExists() {
			list = append(list, metadata)
		}
		if delimiter != "" && regexp.MustCompile(rt+delimiter).MatchString(metadata.Key) {
			prefixes.add(matched + delimiter)
		}
	}

	for _, next := range metadata.NextNodes {
		if !(strings.HasPrefix(prefix, next.Key) || strings.HasPrefix(next.Key, prefix)) {
			continue
		}

		nextMeta, err := n.pool.GetMetadata(ctx, next.NodeId, next.Key)
		if err != nil {
			return nil, nil, err
		}

		s, l, err := n.scan(ctx, prefix, delimiter, limit, nextMeta)
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
