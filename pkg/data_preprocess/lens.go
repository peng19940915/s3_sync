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

	"github.com/peng19940915/s3_sync/pkg/duckdb"
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

func ProcessS3Files(ctx context.Context, opts *options.SyncOptions) error {
	duckCfg := duckdb.Config{
		MemLimit:  opts.DuckDBOpts.MemLimit,
		DBPath:    opts.DuckDBOpts.DBPath,
		Threads:   opts.DuckDBOpts.Threads,
		BatchSize: opts.DuckDBOpts.BatchSize,
	}
	store, err := duckdb.NewDuckStore(duckCfg)
	if err != nil {
		return fmt.Errorf("初始化 DuckDB 失败: %w", err)
	}
	defer store.Close()

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
	jobs := make(chan string, known.PreProcessMaxWorkersForS3)
	results := make(chan []string, known.PreProcessMaxWorkersForS3)
	errChan := make(chan error, known.PreProcessMaxWorkersForS3)

	var wg sync.WaitGroup
	var processedFiles, totalFiles int64
	totalFiles = int64(len(manifest.Files))

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

	// 启动工作协程
	for i := 0; i < known.PreProcessMaxWorkersForS3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				if err := processFile(ctx, client, opts, key, results); err != nil {
					errChan <- err
					return
				}
				atomic.AddInt64(&processedFiles, 1)
			}
		}()
	}

	// 启动写入协程
	var writeWg sync.WaitGroup
	for i := 0; i < known.PreProcessMaxWorkersForS3; i++ {
		writeWg.Add(1)
		go func() {
			defer writeWg.Done()
			for paths := range results {
				select {
				case <-ctx.Done():
					errChan <- ctx.Err()
					return
				default:
					if err := store.WriteBatch(paths); err != nil {
						errChan <- fmt.Errorf("写入DuckDB批量数据失败: %w", err)
						return
					}
				}
			}
		}()
	}

	// Send manifest files to jobs channel
	for _, file := range manifest.Files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobs <- file.Key:
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

	// 所有数据处理完成后，导出排序后的结果
	if err := store.ExportSorted(opts.DataPreprocessOptions.OutputFile); err != nil {
		return fmt.Errorf("导出排序数据失败: %w", err)
	}
	// 输出统计信息
	total, unique, dupRate := store.GetStats()
	fmt.Printf("处理完成:\n总记录数: %d\n唯一记录数: %d\n重复率: %.2f%%\n",
		total, unique, dupRate)

	return nil
}

// 新增函数，用于分批处理文件内容
func processFile(ctx context.Context, client *s3.Client, opts *options.SyncOptions, key string, results chan<- []string) error {
	// 下载文件
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &opts.DataPreprocessOptions.Bucket,
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
	batch := make([]string, 0, opts.DataPreprocessOptions.BatchSize)

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
			if len(batch) >= opts.DataPreprocessOptions.BatchSize {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case results <- batch:
					// 创建新的批次
					batch = make([]string, 0, opts.DataPreprocessOptions.BatchSize)
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
