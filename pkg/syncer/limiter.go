package syncer

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peng19940915/s3_sync/pkg/known"
	"golang.org/x/time/rate"
)

type PathLimiters struct {
	sync.RWMutex
	limiters map[string]*rateLimiterEntry
	count    int32
}

type limiterShard struct {
	sync.RWMutex
	limiters map[string]*rateLimiterEntry
	count    int32
}

type rateLimiterEntry struct {
	limiter    *rate.Limiter
	lastUsed   int64
	createTime int64
	qpsLimit   float64
}

func NewPathLimiters(ctx context.Context) *PathLimiters {
	pl := &PathLimiters{
		limiters: make(map[string]*rateLimiterEntry, 1024),
	}
	go pl.cleanupLoop(ctx)
	return pl
}

func (pl *PathLimiters) GetLimiter(path string) *rate.Limiter {
	now := time.Now().Unix()

	pl.RLock()
	if entry, ok := pl.limiters[path]; ok {
		entry.lastUsed = now
		// 检查是否需要扩容
		if now-entry.createTime >= 30 && entry.qpsLimit < 2000 {
			entry.qpsLimit = math.Min(entry.qpsLimit*2, 2000)
			entry.limiter.SetLimit(rate.Limit(entry.qpsLimit))
			entry.limiter.SetBurst(int(entry.qpsLimit))
			entry.createTime = now
		}
		pl.RUnlock()
		return entry.limiter
	}
	pl.RUnlock()

	pl.Lock()
	defer pl.Unlock()

	entry := &rateLimiterEntry{
		limiter:    rate.NewLimiter(rate.Limit(100), 100),
		lastUsed:   now,
		createTime: now,
		qpsLimit:   100,
	}
	pl.limiters[path] = entry
	atomic.AddInt32(&pl.count, 1)
	return entry.limiter
}

func (pl *PathLimiters) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(known.S3PathShardCleanUpInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pl.cleanup()
			fmt.Println(pl.GetTotalLimiters())
		case <-ctx.Done():
			return
		}
	}
}

func (pl *PathLimiters) cleanup() {
	if atomic.LoadInt32(&pl.count) == 0 {
		return
	}
	now := time.Now().Unix()
	pl.Lock()
	for path, entry := range pl.limiters {
		if now-entry.lastUsed > known.S3PathShardExipireTime {
			delete(pl.limiters, path)
			atomic.AddInt32(&pl.count, -1)
		}
	}
	pl.Unlock()
}

func (pl *PathLimiters) GetTotalLimiters() int32 {
	return atomic.LoadInt32(&pl.count)
}

func (pl *PathLimiters) GetDetail() string {
	var details []string
	pl.RLock()
	for path := range pl.limiters {
		detail := fmt.Sprintf("%s,%d", path, atomic.LoadInt32(&pl.count))
		details = append(details, detail)
	}
	pl.RUnlock()
	return strings.Join(details, "\n")
}
