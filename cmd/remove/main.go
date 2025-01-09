package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/time/rate"
)
/*
根据清单，删除文件
*/
var (
	bucket      string
	keysFile    string
	workerCount int
)

func init() {
	flag.StringVar(&bucket, "bucket", "", "S3存储桶名称")
	flag.StringVar(&keysFile, "keys", "", "包含要删除的S3路径列表的文件")
	flag.IntVar(&workerCount, "workers", 10, "并发工作协程数量")
}

func main() {
	flag.Parse()

	// 验证参数
	if bucket == "" || keysFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	// 读取keys文件
	keys, err := readKeysFile(keysFile)
	if err != nil {
		log.Fatalf("读取keys文件失败: %v", err)
	}

	// 配置AWS客户端
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("无法加载AWS配置: %v", err)
	}

	// 创建S3客户端
	s3Client := s3.NewFromConfig(cfg)

	// 创建限流器 (2000/s)
	limiter := rate.NewLimiter(2000, 2000)

	// 创建进度条
	bar := progressbar.Default(int64(len(keys)))

	// 创建任务通道
	tasks := make(chan string, len(keys))
	var wg sync.WaitGroup
	errorCount := 0
	var errorMutex sync.Mutex

	// 启动工作协程
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range tasks {
				// 等待限流器许可
				err := limiter.Wait(context.TODO())
				if err != nil {
					log.Printf("限流器错误: %v", err)
					continue
				}

				input := &s3.DeleteObjectInput{
					Bucket: &bucket,
					Key:    &key,
				}

				_, err = s3Client.DeleteObject(context.TODO(), input)
				if err != nil {
					errorMutex.Lock()
					errorCount++
					errorMutex.Unlock()
					log.Printf("删除文件失败 %s: %v", key, err)
				}

				bar.Add(1)
			}
		}()
	}

	// 分发任务
	for _, key := range keys {
		tasks <- key
	}
	close(tasks)

	// 等待所有工作协程完成
	wg.Wait()

	fmt.Printf("\n删除完成! 总数: %d, 失败: %d\n", len(keys), errorCount)
}

// readKeysFile 读取包含S3路径的文件，每行一个路径
func readKeysFile(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var keys []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key := strings.TrimSpace(scanner.Text())
		if key != "" {
			keys = append(keys, key)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return keys, nil
}
