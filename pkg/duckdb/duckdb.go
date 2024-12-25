package duckdb

/*
import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb" // 仅注册驱动
)

// DuckStore 封装DuckDB操作
type DuckStore struct {
	db         *sql.DB
	insertStmt *sql.Stmt
	dbPath     string
	batchSize  int
	mu         sync.Mutex
	stats      struct {
		totalRecords  int64 // 处理的总记录数
		uniqueRecords int64 // 唯一记录数
		writeTime     int64 // 写入耗时（纳秒）
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

	// 使用标准 SQL 驱动打开数据库
	db, err := sql.Open("duckdb", cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("打开DuckDB失败: %w", err)
	}

	// 设置连接池参数
	db.SetMaxOpenConns(cfg.Threads)
	db.SetMaxIdleConns(cfg.Threads)

	// 初始化数据库
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS records (
            value VARCHAR NOT NULL,
            PRIMARY KEY(value)
        ) WITH (index_size=1024);
    `)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("初始化DuckDB失败: %w", err)
	}

	// 准备插入语句
	stmt, err := db.Prepare(`
        INSERT INTO records (value)
        VALUES (?)
        ON CONFLICT(value) DO NOTHING
    `)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("准备插入语句失败: %w", err)
	}

	return &DuckStore{
		db:         db,
		insertStmt: stmt,
		dbPath:     cfg.DBPath,
		batchSize:  cfg.BatchSize,
	}, nil
}

// WriteBatch 批量写入数据（自动去重）
func (s *DuckStore) WriteBatch(values []string) error {
	if len(values) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	startTime := time.Now()

	// 开启事务
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback()

	// 预分配内存
	const batchSize = 1000
	placeholders := make([]string, 0, batchSize)
	args := make([]interface{}, 0, batchSize)

	// 分批处理
	for i := 0; i < len(values); i += batchSize {
		end := i + batchSize
		if end > len(values) {
			end = len(values)
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
			return fmt.Errorf("批量插入数据失败: %w", err)
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	atomic.AddInt64(&s.stats.totalRecords, int64(len(values)))
	atomic.AddInt64(&s.stats.writeTime, time.Since(startTime).Nanoseconds())
	return nil
}

// Export 改名为 Export，移除排序逻辑
func (s *DuckStore) Export(outputFilePrefix string) error {
	startTime := time.Now()

	// 获取总记录数
	var totalCount int64
	if err := s.db.QueryRow("SELECT COUNT(*) FROM records").Scan(&totalCount); err != nil {
		return fmt.Errorf("获取记录数失败: %w", err)
	}

	const recordsPerFile = 50_000_000 // 每个文件5000万条记录
	fileCount := (totalCount + recordsPerFile - 1) / recordsPerFile

	// 直接使用 LIMIT 和 OFFSET 进行分页导出
	for fileNumber := int64(1); fileNumber <= fileCount; fileNumber++ {
		fileName := fmt.Sprintf("%s_%03d.txt", outputFilePrefix, fileNumber)
		offset := (fileNumber - 1) * recordsPerFile

		exportQuery := fmt.Sprintf(`
            COPY (
                SELECT value
                FROM records
                LIMIT %d OFFSET %d
            ) TO '%s' (FORMAT CSV);
        `, recordsPerFile, offset, fileName)

		if _, err := s.db.Exec(exportQuery); err != nil {
			return fmt.Errorf("导出数据到文件 %s 失败: %w", fileName, err)
		}

		fmt.Printf("\r导出进度: %.2f%% (文件 %d/%d)", float64(fileNumber)/float64(fileCount)*100, fileNumber, fileCount)
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
	if s.insertStmt != nil {
		s.insertStmt.Close()
	}
	if s.db != nil {
		s.db.Close()
	}
	return os.Remove(s.dbPath)
}
*/
