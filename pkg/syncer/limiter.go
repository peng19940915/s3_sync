package syncer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash"
	"github.com/peng19940915/s3_sync/pkg/known"
	"golang.org/x/time/rate"
)

type PathLimiters struct {
	shards [known.S3PathShardCount]limiterShard
}

type limiterShard struct {
	sync.RWMutex
	limiters map[string]*rateLimiterEntry
	count    int32
}

type rateLimiterEntry struct {
	limiter  *rate.Limiter
	lastUsed int64
}

func NewPathLimiters(ctx context.Context) *PathLimiters {
	pl := &PathLimiters{}
	for i := 0; i < known.S3PathShardCount; i++ {
		pl.shards[i].limiters = make(map[string]*rateLimiterEntry, 1024) // 预分配一定容量
	}
	go pl.cleanupLoop(ctx)
	return pl
}

// 使用 xxHash 进行更快的哈希计算
func (pl *PathLimiters) getShard(path string) *limiterShard {
	h := xxhash.Sum64String(path)
	return &pl.shards[h&known.S3PathShardMask]
}

func (pl *PathLimiters) GetLimiter(path string) *rate.Limiter {
	shard := pl.getShard(path)

	shard.RLock()
	if entry, ok := shard.limiters[path]; ok {
		entry.lastUsed = time.Now().Unix()
		shard.RUnlock()
		return entry.limiter
	}
	shard.RUnlock()
	shard.Lock()
	defer shard.Unlock()
	entry := &rateLimiterEntry{
		limiter:  rate.NewLimiter(rate.Limit(known.S3MaxCopyLimit), known.S3MaxCopyLimit),
		lastUsed: time.Now().Unix(),
	}
	shard.limiters[path] = entry
	atomic.AddInt32(&shard.count, 1)
	return entry.limiter
}

// cleanupLoop 定期清理过期Limiter
func (pl *PathLimiters) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(known.S3PathShardCleanUpInterval * time.Second)
	defer ticker.Stop()
	// 分批清理，避免一次清理太多造成卡顿
	currentShard := 0
	for {
		select {
		case <-ticker.C:
			for i := 0; i < 16; i++ {
				pl.cleanupShard(&pl.shards[currentShard])
				currentShard = (currentShard + 1) % known.S3PathShardCount
			}
			fmt.Println(pl.GetTotalLimiters())
		case <-ctx.Done():
			return
		}
	}
}

// cleanupShard清理分片
func (pl *PathLimiters) cleanupShard(shard *limiterShard) {
	if atomic.LoadInt32(&shard.count) == 0 {
		return
	}
	now := time.Now().Unix()
	shard.Lock()
	for path, entry := range shard.limiters {
		if now-entry.lastUsed > known.S3PathShardExipireTime {
			delete(shard.limiters, path)
			atomic.AddInt32(&shard.count, -1)
		}
	}
	shard.Unlock()
}

// 获取当前限速器总数
func (pl *PathLimiters) GetTotalLimiters() int32 {
	var total int32
	for i := 0; i < known.S3PathShardCount; i++ {
		total += atomic.LoadInt32(&pl.shards[i].count)
	}
	return total
}

func (pl *PathLimiters) GetShardDetail() string {
	var details []string
	for i := 0; i < known.S3PathShardCount; i++ {
		shard := &pl.shards[i]
		shard.RLock()
		for path := range shard.limiters {
			detail := fmt.Sprintf("%d,%s,%d",
				i,
				path,
				atomic.LoadInt32(&shard.count))
			details = append(details, detail)
		}
		shard.RUnlock()
	}
	return strings.Join(details, "\n")
}
