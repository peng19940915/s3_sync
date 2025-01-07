package datapreprocess

/*
./sync -m preprocess --lens-bucket aispace-inventory --lens-output-file keys.txt --lens-prefix dywx-aigc/dywx-aigc/all-inventory/data/
./sync -m preprocess --lens-bucket aispace-inventory --lens-output-file keys.txt --lens-prefix blazers-aigc/blazers-aigc/all-inventory/data/

croc --relay "172.29.0.147:1111" send sync

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build .

./sync -f keys.txt -s blazers-aigc -t mobiu-blazers-aigc -w 10

./sync -f test.txt -s dywx-aigc-temp -t mobiu-dywx-aigc-temp -w 10

CGO_ENABLED=1 CGO_LDFLAGS="-L/home/ec2-user/leiyupeng/duckdb/libs/" go build -tags=duckdb_use_lib .
./sync -m preprocess --lens-bucket aispace-inventory --lens-output-file keys.txt --lens-prefix dywx-aigc/dywx-aigc/all-inventory/data/ --duckdb-mem-limit=16GB --duckdb-threads=1000 --lens-batch-size=200000
*/
import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peng19940915/s3_sync/pkg/options"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/peng19940915/s3_sync/pkg/known"
)

// Add new manifest structure
type ManifestFile struct {
	Key         string `json:"key"`
	Size        int64  `json:"size"`
	MD5checksum string `json:"MD5checksum"`
}

type Manifest struct {
	SourceBucket      string         `json:"sourceBucket"`
	DestinationBucket string         `json:"destinationBucket"`
	Files             []ManifestFile `json:"files"`
}

// Add buffer pools to reuse memory
var (
	batchPool = sync.Pool{
		New: func() interface{} {
			batch := make([]string, 0, known.PreProcessBatchSize)
			return &batch
		},
	}
	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 32*1024) // 32KB buffer
		},
	}
)

func ProcessS3Files(ctx context.Context, opts *options.SyncOptions) error {

	// 配置AWS客户端
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("无法加载AWS配置: %w", err)
	}
	client := s3.NewFromConfig(cfg)

	// Read and parse manifest file
	manifestData, err := os.ReadFile(opts.DataPreprocessOptions.ManifestFile)
	if err != nil {
		return fmt.Errorf("读取manifest文件失败: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return fmt.Errorf("解析manifest文件失败: %w", err)
	}

	// Initialize channels and counters
	jobs := make(chan ManifestFile, known.PreProcessMaxWorkersForS3*2)
	results := make(chan []string, known.PreProcessMaxWorkersForS3*2)
	errChan := make(chan error, 1)

	var wg sync.WaitGroup
	var processedFiles, totalFiles int64
	totalFiles = int64(len(manifest.Files))
	maxWorkers := known.PreProcessMaxWorkersForS3
	if totalFiles <= known.PreProcessMaxWorkersForS3 {
		maxWorkers = int(totalFiles)
	}
	fmt.Printf("total files: %d, max workers: %d\n", totalFiles, maxWorkers)
	// 进度报告协程
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

	// Start worker pool with improved error handling
	workerErrors := make(chan error, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range jobs {
				if err := processFile(ctx, client, opts, file, results); err != nil {
					workerErrors <- err
					return
				}
				atomic.AddInt64(&processedFiles, 1)
			}
		}()
	}

	// 启动单个写入协程
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go func() {
		defer writeWg.Done()
		fileNum := 0
		totalLineCount := 0 // 跟踪总行数，用于文件切分
		//currentBatch := make([]string, 0, known.PreProcessBatchSize)

		// 创建新文件并获取writer
		createNewFile := func() (*bufio.Writer, *os.File, error) {
			fileNum++
			outputPath := fmt.Sprintf("%s.part_%d", opts.DataPreprocessOptions.OutputFile, fileNum)
			f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				return nil, nil, fmt.Errorf("create output file failed %s: %w", outputPath, err)
			}
			return bufio.NewWriterSize(f, 1024*1024), f, nil // 1MB buffer
		}

		writer, file, err := createNewFile()
		if err != nil {
			errChan <- err
			return
		}
		defer file.Close()

		for paths := range results {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				for _, line := range paths {
					if totalLineCount >= opts.DataPreprocessOptions.FileBatchSize {
						if err := writer.Flush(); err != nil {
							errChan <- fmt.Errorf("flush error: %w", err)
							return
						}
						file.Close()

						writer, file, err = createNewFile()
						if err != nil {
							errChan <- err
							return
						}
						totalLineCount = 0
					}

					if _, err := writer.WriteString(line + "\n"); err != nil {
						errChan <- err
						return
					}
					totalLineCount++
				}

				if err := writer.Flush(); err != nil {
					errChan <- fmt.Errorf("flush error: %w", err)
					return
				}
			}
		}
	}()

	// Send manifest files to jobs channel
	for _, file := range manifest.Files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobs <- file:
		}
	}

	// 错误检查函数
	checkError := func() error {
		select {
		case err := <-errChan:
			return err
		default:
			return nil
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

	return nil
}

// Add new helper function to write batch to file
func writeToFile(filepath string, lines []string) error {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create output file failed %s: %w", filepath, err)
	}
	defer f.Close()

	// Use larger buffer for writing
	writer := bufio.NewWriterSize(f, 1024*1024) // 1MB buffer
	defer writer.Flush()

	for _, line := range lines {
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("write to file failed %s: %w", filepath, err)
		}
	}
	return nil
}

// 新增函数，用分批处理文件内容
func processFile(ctx context.Context, client *s3.Client, opts *options.SyncOptions, file ManifestFile, results chan<- []string) error {
	// Get buffer from pool
	buffer := bufferPool.Get().([]byte)
	defer bufferPool.Put(buffer)

	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &opts.DataPreprocessOptions.Bucket,
		Key:    &file.Key,
	})
	if err != nil {
		return fmt.Errorf("download failed for %s: %w", file.Key, err)
	}
	defer resp.Body.Close()

	// Use buffered reader with pooled buffer
	reader := bufio.NewReaderSize(resp.Body, len(buffer))
	gzReader, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("create gzip reader failed for %s: %w", file.Key, err)
	}
	defer gzReader.Close()

	csvReader := csv.NewReader(gzReader)
	batch := *batchPool.Get().(*[]string)
	defer batchPool.Put(&batch)
	batch = batch[:0] // Reset batch

	// 逐行读取并批量发送
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("读取CSV %s 失败: %w", file.Key, err)
		}

		if len(record) >= 2 {
			batch = append(batch, record[1])

			if len(batch) >= known.PreProcessBatchSize {
				// 创建批次的副本
				batchCopy := make([]string, len(batch))
				copy(batchCopy, batch)

				select {
				case <-ctx.Done():
					return ctx.Err()
				case results <- batchCopy:
					batch = batch[:0] // 重置批次，而不是创建新的
				}
			}
		}
	}

	// 处理最后的批次
	if len(batch) > 0 {
		batchCopy := make([]string, len(batch))
		copy(batchCopy, batch)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case results <- batchCopy:
		}
	}

	return nil
}
