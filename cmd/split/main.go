package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"

	"github.com/peng19940915/s3_sync/pkg/duckdb"
)

func main() {
	var (
		dbPath    = flag.String("db", "data.db", "DuckDB数据库文件路径")
		outputDir = flag.String("output", "output", "输出目录")
		batchSize = flag.Int("batch", 100000, "批处理大小")
		memLimit  = flag.String("mem", "4GB", "内存限制")
		threads   = flag.Int("threads", 4, "并发线程数")
	)
	flag.Parse()

	// 确保输出目录存在
	outputPrefix := filepath.Join(*outputDir, "part")

	// 初始化DuckDB存储
	store, err := duckdb.NewDuckStore(duckdb.Config{
		DBPath:    *dbPath,
		BatchSize: *batchSize,
		MemLimit:  *memLimit,
		Threads:   *threads,
	})
	if err != nil {
		log.Fatalf("初始化DuckDB失败: %v", err)
	}
	defer store.Close()

	// 导出排序后的数据
	if err := store.ExportSorted(outputPrefix); err != nil {
		log.Fatalf("导出数据失败: %v", err)
	}

	// 输出统计信息
	total, unique, dupRate := store.GetStats()
	fmt.Printf("\n处理统计:\n")
	fmt.Printf("总记录数: %d\n", total)
	fmt.Printf("唯一记录数: %d\n", unique)
	fmt.Printf("重复率: %.2f%%\n", dupRate)
}
