package syncer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	news3 "github.com/peng19940915/s3_sync/pkg/aws/s3"
	"github.com/peng19940915/s3_sync/pkg/known"
	"github.com/peng19940915/s3_sync/pkg/options"
	"github.com/peng19940915/s3_sync/pkg/utils"
	"github.com/schollz/progressbar/v3"
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
	recordFile   string
	limiter      *rate.Limiter
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
		recordFile:   opts.RecordFile,
		limiter:      rate.NewLimiter(rate.Limit(known.S3MaxCopyLimit), known.S3MaxCopyLimit), // 每秒2000个请求
	}
	s.loadCopiedKeys(fmt.Sprintf("%s_%s.txt", known.SuccessRecordPath, s.sourceBucket))
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
func (s *Syncer) saveKey(status string, keys <-chan string) error {
	filename := fmt.Sprintf("%s_%s.txt", known.SuccessRecordPath, s.sourceBucket)
	if status == known.FailedSyncStatus {
		filename = fmt.Sprintf("%s_%s.txt", known.FailedRecordPath, s.sourceBucket)
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
	for key := range keys {
		if status == known.SuccessSyncStatus {
			if _, ok := s.copiedKeys.Load(key); ok {
				return nil
			}
		}
		if _, err := writer.WriteString(key + "\n"); err != nil {
			return err
		}
	}
	return nil
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
			if _, ok := s.copiedKeys.Load(*obj.Key); ok {
				continue
			}
			select {
			case tasks <- *obj.Key:
			case <-ctx.Done():
				return nil
			}
		}
	}
	return nil
}

// 从文件中加载需要复制的key
func (s *Syncer) listObjectsFromFile(ctx context.Context, tasks chan<- string) error {
	totalLines, err := utils.CountFileLines(s.recordFile)
	if err != nil {
		return fmt.Errorf("failed to count lines: %w", err)
	}

	file, err := os.Open(s.recordFile)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, known.FileBufferSize)
	// 创建进度条
	bar := progressbar.NewOptions(totalLines,
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(), // 显示速率
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("Syncing"),
		progressbar.OptionThrottle(65*time.Millisecond), // 限制更新频率
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read line: %w", err)
		}
		if isPrefix {
			var fullLine []byte
			fullLine = append(fullLine, line...)
			for isPrefix {
				line, isPrefix, err = reader.ReadLine()
				if err != nil {
					return fmt.Errorf("failed to read line: %w", err)
				}
				fullLine = append(fullLine, line...)
			}
			line = fullLine
		}
		key := strings.TrimSpace(string(line))
		if key == "" {
			continue
		}
		if _, ok := s.copiedKeys.Load(key); ok {
			continue
		}
		select {
		case tasks <- key:
		case <-ctx.Done():
			return nil
		}
		bar.Add(1)
	}
	hlog.Infof("Finished load all key from file:%s", s.recordFile)
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
			hlog.Errorf("failed to copy object %s: %v, retrying...", key, err)
			// 返回 false 继续重试
			return false, nil
		}
		// 成功时返回 true
		return true, nil
	})
}

func (s *Syncer) copyObject(ctx context.Context, key string) error {
	if err := s.limiter.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait for limiter: %w", err)
	}
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

func (s *Syncer) Run(ctx context.Context, opts *options.SyncOptions) error {
	var successCount, failureCount int64
	ticker := time.NewTicker(time.Minute)
	g, ctx := errgroup.WithContext(ctx)
	keys := make(chan string, s.workers*2)
	copiedKeys := make(chan string, s.workers*2)
	failedKeys := make(chan string, s.workers*2)

	// Log the counts every minute
	go func() error {
		for {
			select {
			case <-ticker.C:
				if s.recordFile == "" {
					hlog.Infof("Cumulative Success: %d, Cumulative Failures: %d", successCount, failureCount)
				}
			case <-ctx.Done():
				return nil
			}
		}
	}()

	// Start listing objects
	go func() error {
		var err error
		if opts.RecordFile != "" {
			err = s.listObjectsFromFile(ctx, keys)
		} else {
			err = s.listObjects(ctx, keys)
		}
		close(keys)
		return err
	}()

	for i := 0; i < s.workers; i++ {
		g.Go(func() error {
			for key := range keys {
				if err := s.CopyObjectWithRetry(ctx, key); err != nil {
					if err != context.Canceled {
						failedKeys <- key
						atomic.AddInt64(&failureCount, 1)
						hlog.Errorf("failed to copy object %s: %v", key, err)
					}
					continue
				}
				copiedKeys <- key
				atomic.AddInt64(&successCount, 1)
			}
			return nil
		})
	}

	// Wait for all workers to finish, then close result channels
	defer func() {
		ticker.Stop()
		hlog.Infof("Cumulative Success: %d, Cumulative Failures: %d", successCount, failureCount)
	}()

	saveWaitGroup := sync.WaitGroup{}
	// Channel handlers remain the same
	saveWaitGroup.Add(1)
	go func() error {
		defer saveWaitGroup.Done()
		return s.saveKey(known.SuccessSyncStatus, copiedKeys)
	}()
	saveWaitGroup.Add(1)
	go func() error {
		defer saveWaitGroup.Done()
		return s.saveKey(known.FailedSyncStatus, failedKeys)
	}()
	err := g.Wait()
	close(copiedKeys)
	close(failedKeys)
	saveWaitGroup.Wait()
	return err
}
