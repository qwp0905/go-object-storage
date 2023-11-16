package locker

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

func readLockKey(key string) string {
	return fmt.Sprintf("READ:%s", key)
}

const readLockScript = `
if redis.call("EXISTS", KEYS[1]) == 1 then
	return redis.call("PTTL", KEYS[1])
end
redis.call("INCR", KEYS[2])
redis.call("PEXPIRE", KEYS[2], ARGV[1])
return -10
`

const readUnlockScript = `
if redis.call("DECR", KEYS[2]) ~= 0 then
	return 1
end
if redis.call("DEL", KEYS[2]) == 1 then
	return redis.call("PUBLISH", KEYS[1], "")
end
return 0
`

const writeLockScript = `
if redis.call("EXISTS", KEYS[2]) == 1 then
	return redis.call("PTTL", KEYS[2])
end
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
	return -10
end
return redis.call("PTTL", KEYS[1])
`

const writeUnlockScript = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
if redis.call("DEL", KEYS[1]) == 1 then
	return redis.call("PUBLISH", KEYS[1], "")
end
return 0
`

type RWMutex struct {
	rc      *redis.Client
	key     string
	current string
	timeout time.Duration
}

func NewRWMutex(rc *redis.Client, key string, timeout time.Duration) *RWMutex {
	return &RWMutex{rc: rc, timeout: timeout}
}

func (l *RWMutex) RLock(ctx context.Context) error {
	sub := l.rc.Subscribe(ctx, l.key)
	defer sub.Close()
	for {
		pttl, err := l.rc.Eval(
			ctx,
			readLockScript,
			[]string{l.key, readLockKey(l.key)},
			l.timeout.Milliseconds(),
		).Int()
		if err != nil {
			return errors.WithStack(err)
		}
		if pttl == -10 {
			return nil
		}
		select {
		case <-sub.Channel():
		case <-time.After(time.Duration(pttl) * time.Millisecond):
		}
	}
}

func (l *RWMutex) RUnlock(ctx context.Context) error {
	return errors.WithStack(l.rc.Eval(
		ctx,
		readUnlockScript,
		[]string{l.key, readLockKey(l.key)},
	).Err())
}

func generate() string {
	return uuid.Must(uuid.NewRandom()).String()
}

func (l *RWMutex) Lock(ctx context.Context) error {
	v := generate()
	sub := l.rc.Subscribe(ctx, l.key)
	defer sub.Close()
	for {
		pttl, err := l.rc.Eval(
			ctx,
			writeLockScript,
			[]string{l.key, readLockKey(l.key)},
			v,
			l.timeout.Milliseconds(),
		).Int()
		if err != nil {
			return errors.WithStack(err)
		}
		if pttl == -10 {
			l.current = v
			return nil
		}
		select {
		case <-sub.Channel():
		case <-time.After(time.Duration(pttl) * time.Millisecond):
		}
	}
}

func (l *RWMutex) Unlock(ctx context.Context) error {
	return errors.WithStack(l.rc.Eval(
		ctx,
		writeUnlockScript,
		[]string{l.key},
		l.current,
	).Err())
}
