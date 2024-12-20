package syncer

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	news3 "github.com/peng19940915/s3_sync/pkg/aws/s3"
	"github.com/peng19940915/s3_sync/pkg/known"
	"github.com/peng19940915/s3_sync/pkg/options"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/util/wait"
)

type Syncer struct {
	client       *s3.Client
	sourceBucket string
	targetBucket string
	region       string
	workers      int
	prefix       string
	copiedKeys   sync.Map

	limiter *rate.Limiter
}

// 任务对象池
var objectPool = sync.Pool{
	New: func() interface{} {
		return &types.Object{}
	},
}

func NewSyncer(opts *options.SyncOptions) *Syncer {
	var s = &Syncer{
		client:       news3.NewClient(opts.Region),
		sourceBucket: opts.SourceBucket,
		targetBucket: opts.TargetBucket,
		region:       opts.Region,
		workers:      opts.Workers,
		prefix:       opts.Prefix,
		//limiter:      rate.NewLimiter(rate.Limit(1000), 1000), // 每秒1000个请求
	}
	s.loadCopiedKeys(fmt.Sprintf("%s_%s", known.SuccessRecordPath, s.sourceBucket))
	return s
}

// Load copied keys from a file
func (s *Syncer) loadCopiedKeys(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key := strings.TrimSpace(scanner.Text())
		s.copiedKeys.Store(key, struct{}{})
	}
	return scanner.Err()
}

// Save a copied key to a file
func (s *Syncer) saveKey(ctx context.Context, status string, keys <-chan string) error {
	filename := fmt.Sprintf("%s_%s", known.SuccessRecordPath, s.sourceBucket)
	if status == known.FailedSyncStatus {
		filename = fmt.Sprintf("%s_%s", known.FailedRecordPath, s.sourceBucket)
	}
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(file)
	defer func() {
		hlog.Infof("flush data to: %s", filename)
		writer.Flush() // 确保在关闭文件前刷新缓冲区
		file.Close()   // 确保文件被关闭
	}()

	for {
		select {
		case key, ok := <-keys:
			if !ok {
				return nil
			}
			if status == known.SuccessSyncStatus {
				if _, ok := s.copiedKeys.Load(key); ok {
					continue
				}
			}
			if _, err := writer.WriteString(key + "\n"); err != nil {
				return err
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (s *Syncer) listObjects(ctx context.Context, tasks chan<- string) error {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:     &s.sourceBucket,
		MaxKeys:    aws.Int32(1000),
		FetchOwner: aws.Bool(false), // 不获取所有者信息，减少响应大小
		Prefix:     &s.prefix,
	})
	for paginator.HasMorePages() {
		// 获取下一页对象
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		// 将对象加入任务队列
		for _, obj := range output.Contents {
			select {
			case tasks <- *obj.Key:
			case <-ctx.Done():
				return nil
			}
		}
	}
	return nil
}

// 重试调用s3 copy
func (s *Syncer) CopyObjectWithRetry(ctx context.Context, key string) error {
	// 定义退避策略
	return wait.ExponentialBackoff(known.DefaultBackoff, func() (bool, error) {
		// 如果上下文取消，直接返回错误
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		// 尝试复制对象
		if err := s.copyObject(ctx, key); err != nil {
			// 返回 false 继续重试
			return false, nil
		}
		// 成功时返回 true
		return true, nil
	})
}

func (s *Syncer) copyObject(ctx context.Context, key string) error {
	source := fmt.Sprintf("%s/%s", s.sourceBucket, key)
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            &s.targetBucket,
		CopySource:        &source,
		Key:               &key,
		MetadataDirective: types.MetadataDirectiveCopy,
	})

	if err != nil {
		//var apiErr smithy.APIError
		//if errors.As(err, &apiErr) {
		//	switch {
		//	case apiErr.ErrorCode() == "EntityTooLarge":
		//		fmt.Printf("File exceeds 5GB limit: %s (size: %d bytes)\n", task.Key, task.Size)
		// 这里可以添加记录逻辑
		//	case apiErr.ErrorCode() == "InvalidRequest":
		//		// 处理其他无效请求错误
		//	case apiErr.ErrorCode() == "NoSuchKey":
		//		// 处理源文件不存在的情况
		//	}
		//}
		//TODO 记录错误
		return fmt.Errorf("failed to copy object %s: %w", key, err)
	}

	return nil
}

func (s *Syncer) Run(ctx context.Context) error {
	var successCount, failureCount int64
	ticker := time.NewTicker(time.Minute)

	g, ctx := errgroup.WithContext(ctx)
	keys := make(chan string, s.workers*2)
	copiedKeys := make(chan string, 1000) // 用于存储已复制键的缓冲通道
	failedKeys := make(chan string, 1000) // 用于存储失败键的缓冲通道
	defer func() {
		fmt.Println("relase source channel")
		close(keys)
		close(copiedKeys)
		close(failedKeys)
		defer ticker.Stop()
	}()
	// Log the counts every minute
	g.Go(func() error {
		for {
			select {
			case <-ticker.C:
				hlog.Infof("Cumulative Success: %d, Cumulative Failures: %d", successCount, failureCount)
			case <-ctx.Done():
				return nil
			}
		}
	})
	g.Go(func() error {
		return s.listObjects(ctx, keys)
	})
	g.Go(func() error {
		return s.saveKey(ctx, known.SuccessSyncStatus, copiedKeys)
	})
	g.Go(func() error {
		return s.saveKey(ctx, known.FailedSyncStatus, failedKeys)
	})

	for i := 0; i < s.workers; i++ {
		g.Go(func() error {
			for {
				select {
				case key, ok := <-keys:
					if !ok {
						return nil
					}
					if err := s.CopyObjectWithRetry(ctx, key); err != nil {
						if err != context.Canceled {
							//s.failedKeys.Store(key, struct{}{})
							failedKeys <- key
						}
						continue
					}
					//s.copiedKeys.Store(key, struct{}{})
					copiedKeys <- key
				case <-ctx.Done():
					return nil
				}
			}
		})
	}
	return g.Wait()
}
