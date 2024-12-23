package datapreprocess

/*
./sync -m preprocess --lens-bucket aispace-inventory --lens-output-file keys.txt --lens-prefix dywx-aigc/dywx-aigc/all-inventory/data/
./sync -m preprocess --lens-bucket aispace-inventory --lens-output-file keys.txt --lens-prefix blazers-aigc/blazers-aigc/all-inventory/data/

croc --relay "172.29.0.147:1111" send sync

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build .

./sync -f keys.txt -s blazers-aigc -t mobiu-blazers-aigc -w 10

./sync -f test.txt -s dywx-aigc-temp -t mobiu-dywx-aigc-temp -w 10

*/
import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	maxWorkers  = 10        // 并发下载的worker数量
	bufferSize  = 32 * 1024 // 32KB buffer
	outputBatch = 1000      // 输出缓冲行数
)

func ProcessS3Files(ctx context.Context, bucket, prefix, outputFile string) error {
	// 配置AWS客户端
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("无法加载AWS配置: %w", err)
	}
	client := s3.NewFromConfig(cfg)

	// 创建输出文件
	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %w", err)
	}
	defer output.Close()

	writer := bufio.NewWriterSize(output, bufferSize)
	defer writer.Flush()

	// 简化为只使用必要的通道
	jobs := make(chan string, maxWorkers)
	results := make(chan []string, maxWorkers)
	errChan := make(chan error, maxWorkers) // 统一使用一个错误通道

	var wg sync.WaitGroup
	var processedFiles, totalFiles int64

	// 进度报告协程保持不变
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processed := atomic.LoadInt64(&processedFiles)
				total := atomic.LoadInt64(&totalFiles)
				if total > 0 {
					fmt.Printf("进度: %.2f%% (%d/%d 文件已处理)\n",
						float64(processed)/float64(total)*100,
						processed, total)
				}
			}
		}
	}()

	// 启动工作协程
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				if err := processFile(ctx, client, bucket, key, results); err != nil {
					errChan <- err
					return
				}
				atomic.AddInt64(&processedFiles, 1)
			}
		}()
	}

	// 启动写入协程
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go func() {
		defer writeWg.Done()
		buffer := make([]string, 0, outputBatch)

		for paths := range results {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				buffer = append(buffer, paths...)
				if len(buffer) >= outputBatch {
					if err := writeLines(writer, buffer); err != nil {
						errChan <- err
						return
					}
					buffer = buffer[:0]
				}
			}
		}

		// 写入剩余的数据
		if len(buffer) > 0 {
			if err := writeLines(writer, buffer); err != nil {
				errChan <- err
			}
		}
	}()

	// 发送任务
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:  &bucket,
		Prefix:  &prefix,
		MaxKeys: aws.Int32(1000),
	})

	// 错误检查函数
	checkError := func() error {
		select {
		case err := <-errChan:
			return err
		default:
			return nil
		}
	}

	// 发送任务并检查错误
	for paginator.HasMorePages() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("列出S3对象失败: %w", err)
			}

			for _, obj := range page.Contents {
				if strings.HasSuffix(*obj.Key, ".gz") {
					atomic.AddInt64(&totalFiles, 1)

					// 检查是否有错误发生
					if err := checkError(); err != nil {
						return fmt.Errorf("处理失败: %w", err)
					}

					jobs <- *obj.Key
				}
			}
		}
	}

	// 关闭任务通道并等待所有处理完成
	close(jobs)
	wg.Wait()
	close(results)
	writeWg.Wait()

	// 最后检查是否有错误
	if err := checkError(); err != nil {
		return fmt.Errorf("处理失败: %w", err)
	}
	fmt.Println("处理完成")
	return nil
}

func processS3Object(ctx context.Context, client *s3.Client, bucket string, jobs <-chan string, results chan<- []string, errors chan<- error,
	currentFiles map[string]bool, currentFilesMutex *sync.Mutex, processedFiles *int64) {
	for key := range jobs {
		select {
		case <-ctx.Done():
			errors <- ctx.Err()
			return
		default:
			fmt.Printf("正在处理文件: %s\n", key)
			currentFilesMutex.Lock()
			currentFiles[key] = true
			currentFilesMutex.Unlock()

			// 下载并处理文件
			if err := processFile(ctx, client, bucket, key, results); err != nil {
				errors <- err
				return
			}

			// 从当前处理文件列表中移除
			currentFilesMutex.Lock()
			delete(currentFiles, key)
			currentFilesMutex.Unlock()

			// 更新处理文件计数
			atomic.AddInt64(processedFiles, 1)
		}
	}
}

// 新增函数，用于分批处理文件内容
func processFile(ctx context.Context, client *s3.Client, bucket, key string, results chan<- []string) error {
	// 下载文件
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("下载文件 %s 失败: %w", key, err)
	}
	defer resp.Body.Close()

	// 创建gzip reader
	gzReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("创建gzip reader失败 %s: %w", key, err)
	}
	defer gzReader.Close()

	// 创建CSV reader
	csvReader := csv.NewReader(gzReader)

	// 使用固定大小的批处理
	batch := make([]string, 0, outputBatch)

	// 逐行读取并批量发送
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取CSV %s 失败: %w", key, err)
		}

		if len(record) >= 2 {
			batch = append(batch, record[1])

			// 当批次达到指定大小时发送
			if len(batch) >= outputBatch {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case results <- batch:
					// 创建新的批次
					batch = make([]string, 0, outputBatch)
				}
			}
		}
	}

	// 发送最后的批次
	if len(batch) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case results <- batch:
		}
	}
	fmt.Println(fmt.Sprintf("处理完成 %s", key))
	return nil
}

func writeLines(writer *bufio.Writer, lines []string) error {
	for _, line := range lines {
		if _, err := fmt.Fprintln(writer, line); err != nil {
			return err
		}
	}
	return writer.Flush()
}
