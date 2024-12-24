package duckdb

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb" // 仅注册驱动
)

// DuckStore 封装DuckDB操作
type DuckStore struct {
	conns       []*sql.DB   // 连接池
	insertStmts []*sql.Stmt // 每个连接对应的预处理语句
	dbPath      string
	batchSize   int
	connMu      []sync.Mutex // 每个连接一个互斥锁
	stats       struct {
		totalRecords  int64
		uniqueRecords int64
		writeTime     int64
		connUsage     []int64 // 每个连接的使用次数
	}
}

// Config DuckDB配置选项
type Config struct {
	DBPath    string
	BatchSize int
	MemLimit  string
	Threads   int
}

// NewDuckStore 创建新的DuckDB存储实例
func NewDuckStore(cfg Config) (*DuckStore, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100000
	}
	if cfg.Threads <= 0 {
		cfg.Threads = 100 // 默认100个连接
	}

	store := &DuckStore{
		conns:       make([]*sql.DB, cfg.Threads),
		insertStmts: make([]*sql.Stmt, cfg.Threads),
		connMu:      make([]sync.Mutex, cfg.Threads),
		dbPath:      cfg.DBPath,
		batchSize:   cfg.BatchSize,
	}
	store.stats.connUsage = make([]int64, cfg.Threads)

	// 初始化第一个连接并创建表
	db0, err := sql.Open("duckdb", fmt.Sprintf("%s?threads=%d", cfg.DBPath, 4)) // 每个连接4个线程
	if err != nil {
		return nil, fmt.Errorf("打开DuckDB失败: %w", err)
	}

	// 设置连接参数
	db0.SetMaxOpenConns(1) // 每个连接实例只允许一个活动连接
	db0.SetMaxIdleConns(1)
	db0.SetConnMaxLifetime(time.Hour)

	// 创建表结构
	_, err = db0.Exec(`
        CREATE TABLE IF NOT EXISTS records (
            value VARCHAR NOT NULL,
            PRIMARY KEY(value)
        ) WITH (index_size=32768);
    `)
	if err != nil {
		db0.Close()
		return nil, fmt.Errorf("初始化DuckDB失败: %w", err)
	}

	store.conns[0] = db0
	stmt0, err := db0.Prepare(`
        INSERT INTO records (value)
        VALUES (?)
        ON CONFLICT(value) DO NOTHING
    `)
	if err != nil {
		db0.Close()
		return nil, fmt.Errorf("准备插入语句失败: %w", err)
	}
	store.insertStmts[0] = stmt0

	// 初始化其余连接
	for i := 1; i < cfg.Threads; i++ {
		db, err := sql.Open("duckdb", fmt.Sprintf("%s?threads=%d", cfg.DBPath, 4))
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("打开DuckDB连接 %d 失败: %w", i, err)
		}

		// 设置连接参数
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(time.Hour)

		store.conns[i] = db

		stmt, err := db.Prepare(`
            INSERT INTO records (value)
            VALUES (?)
            ON CONFLICT(value) DO NOTHING
        `)
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("准备插入语句 %d 失败: %w", i, err)
		}
		store.insertStmts[i] = stmt
	}

	// 启动监控协程
	go store.monitorConnections()

	return store, nil
}

// 监控连接使用情况
func (s *DuckStore) monitorConnections() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		var total int64
		for i, usage := range s.stats.connUsage {
			count := atomic.LoadInt64(&usage)
			total += count
			if count > 0 {
				fmt.Printf("连接 %d 使用次数: %d\n", i, count)
			}
		}
		fmt.Printf("总写入次数: %d, 平均每个连接: %.2f\n",
			total, float64(total)/float64(len(s.conns)))
	}
}

// 获取最少使用的连接索引
func (s *DuckStore) getLeastUsedConnIndex() int {
	// 随机选择几个连接进行比较，而不是遍历所有连接
	const sampleSize = 3
	connCount := len(s.stats.connUsage)

	minIndex := rand.Intn(connCount)
	minUsage := atomic.LoadInt64(&s.stats.connUsage[minIndex])

	for i := 0; i < sampleSize-1; i++ {
		idx := rand.Intn(connCount)
		usage := atomic.LoadInt64(&s.stats.connUsage[idx])
		if usage < minUsage {
			minUsage = usage
			minIndex = idx
		}
	}

	return minIndex
}

// WriteBatch 批量写入数据（自动去重）
func (s *DuckStore) WriteBatch(values []string) error {
	if len(values) == 0 {
		return nil
	}

	// 选择连接的操作移到锁外面
	connIndex := s.getLeastUsedConnIndex()
	atomic.AddInt64(&s.stats.connUsage[connIndex], 1)

	// 预分配内存也移到锁外面
	batchSize := s.batchSize // 使用配置的batchSize
	placeholders := make([]string, 0, batchSize)
	args := make([]interface{}, 0, batchSize)

	// 缩小锁的范围，只在实际写入时加锁
	s.connMu[connIndex].Lock()
	defer s.connMu[connIndex].Unlock()

	startTime := time.Now()

	// 每个批次都使用独立的事务
	for i := 0; i < len(values); i += batchSize {
		end := i + batchSize
		if end > len(values) {
			end = len(values)
		}

		// 为每个批次开启新事务
		tx, err := s.conns[connIndex].Begin()
		if err != nil {
			return fmt.Errorf("开启事务失败: %w", err)
		}

		// 重置切片
		placeholders = placeholders[:0]
		args = args[:0]

		// 构建当前批次的参数
		for _, value := range values[i:end] {
			placeholders = append(placeholders, "(?)")
			args = append(args, value)
		}

		query := fmt.Sprintf(`
            INSERT INTO records (value)
            VALUES %s
            ON CONFLICT(value) DO NOTHING
        `, strings.Join(placeholders, ","))

		if _, err := tx.Exec(query, args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("批量插入数据失败: %w", err)
		}

		// 立即提交这个批次的事务
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("提交事务失败: %w", err)
		}
	}

	atomic.AddInt64(&s.stats.totalRecords, int64(len(values)))
	atomic.AddInt64(&s.stats.writeTime, time.Since(startTime).Nanoseconds())
	return nil
}

// ExportSorted 导出排序后的数据到多个文件
func (s *DuckStore) ExportSorted(outputFilePrefix string) error {
	startTime := time.Now()

	// 获取总记录数
	var totalCount int64
	if err := s.conns[0].QueryRow("SELECT COUNT(*) FROM records").Scan(&totalCount); err != nil {
		return fmt.Errorf("获取记录数失败: %w", err)
	}

	const recordsPerFile = 50_000_000 // 每个文件5000万条记录
	fileCount := (totalCount + recordsPerFile - 1) / recordsPerFile

	for i := int64(0); i < fileCount; i++ {
		fileName := fmt.Sprintf("%s_%03d.txt", outputFilePrefix, i+1)

		// 使用COPY命令导出当前批次
		query := fmt.Sprintf(`
            COPY (
                SELECT value 
                FROM records 
                ORDER BY value
                LIMIT %d OFFSET %d
            ) TO '%s' (FORMAT CSV)
        `, recordsPerFile, i*recordsPerFile, fileName)

		if _, err := s.conns[0].Exec(query); err != nil {
			return fmt.Errorf("导出数据到文件 %s 失败: %w", fileName, err)
		}

		fmt.Printf("\r导出进度: %.2f%% (文件 %d/%d)", float64(i+1)/float64(fileCount)*100, i+1, fileCount)
	}
	fmt.Println() // 换行

	// 更新统计信息
	atomic.StoreInt64(&s.stats.uniqueRecords, totalCount)

	fmt.Printf("导出完成，总记录数：%d，总文件数：%d，耗时：%v\n",
		totalCount, fileCount, time.Since(startTime))
	return nil
}

// GetStats 获取处理统计信息
func (s *DuckStore) GetStats() (totalRecords, uniqueRecords int64, duplicateRate float64) {
	total := atomic.LoadInt64(&s.stats.totalRecords)
	unique := atomic.LoadInt64(&s.stats.uniqueRecords)
	if total > 0 {
		duplicateRate = float64(total-unique) / float64(total) * 100
	}
	return total, unique, duplicateRate
}

// Close 关闭数据库连接并清理临时文件
func (s *DuckStore) Close() error {
	for _, db := range s.conns {
		if db != nil {
			db.Close()
		}
	}
	return os.Remove(s.dbPath)
}
