package locker

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func readLockKey(key string) string {
	return fmt.Sprintf("READ:%s", key)
}

func writeLockKey(key string) string {
	return fmt.Sprintf("WRITE:%s", key)
}

const readLockScript = `
if redis.call("EXISTS", KEYS[2]) then
	return redis.call("PTTL", KEYS[2])
end
redis.call("RPUSH", KEYS[1], ARGV[1])
redis.call("PTTL", ARGV[2])
return -10
`

const readUnlockScript = `
if redis.call("GET", KEYS[1])[0] ~= ARGV[1] then
	return nil
end
`

// TODO rwlock 구현 필요할수도... 최소한 그냥 locker라도
type RWMutex struct {
	rc      *redis.Client
	key     string
	current string
}

func NewRWMutex(rc *redis.Client, key string) *RWMutex {
	return &RWMutex{rc: rc}
}

func (l *RWMutex) Lock(ctx context.Context) error {
	sub := l.rc.Subscribe(ctx, l.key)
	defer sub.Close()
	for {
		l.rc.Eval(ctx, acquireScript, []string{}).Int()
	}
}
