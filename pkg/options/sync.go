package options

import (
	"errors"
	"os"

	"github.com/peng19940915/s3_sync/pkg/known"
	"github.com/spf13/pflag"
)

type SyncOptions struct {
	SourceBucket          string
	TargetBucket          string
	ShowVersion           bool
	Region                string
	Workers               int
	Prefix                string
	RecordFile            string
	ServerOpts            ServerOptions
	DataPreprocessOptions DataPreprocessOptions
	Mode                  string
	DuckDBOpts            DuckDBOptions
}

type ServerOptions struct {
	BindPort    int
	PprofEnable bool
}

type DataPreprocessOptions struct {
	Bucket     string
	BatchSize  int
	OutputFile string
	Prefix     string
}

type DuckDBOptions struct {
	MemLimit  string
	DBPath    string
	Threads   int
	BatchSize int
}

func NewSyncOptions() *SyncOptions {
	return &SyncOptions{}
}

func (o *SyncOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVarP(&o.Mode, "mode", "m", "sync", "Mode")
	fs.StringVarP(&o.SourceBucket, "source-bucket", "s", "null", "Source bucket")
	fs.StringVarP(&o.TargetBucket, "target-bucket", "t", "null", "Target bucket")
	fs.BoolVarP(&o.ShowVersion, "version", "v", false, "Show version")
	fs.StringVarP(&o.Region, "region", "r", "us-east-1", "Region")
	fs.IntVarP(&o.Workers, "workers", "w", 10, "Workers")
	fs.StringVarP(&o.Prefix, "prefix", "p", "", "Prefix")
	fs.StringVarP(&o.RecordFile, "record-file", "f", "", "Record file")
	fs.IntVarP(&o.ServerOpts.BindPort, "bind-port", "b", 8080, "server port")
	fs.BoolVar(&o.ServerOpts.PprofEnable, "pprof", false, "pprof")
	// data preprocess for storage lens
	fs.StringVar(&o.DataPreprocessOptions.Bucket, "lens-bucket", "", "Input Bucket")
	fs.StringVar(&o.DataPreprocessOptions.OutputFile, "lens-output-file", "", "Output file")
	fs.StringVar(&o.DataPreprocessOptions.Prefix, "lens-prefix", "", "Storage Lens prefix")
	fs.IntVar(&o.DataPreprocessOptions.BatchSize, "lens-batch-size", 50000, "Storage Lens batch size")
	// duckdb
	fs.StringVar(&o.DuckDBOpts.MemLimit, "duckdb-mem-limit", "8GB", "DuckDB mem limit")
	fs.StringVar(&o.DuckDBOpts.DBPath, "duckdb-db-path", "duckdb.db", "DuckDB db path")
	fs.IntVar(&o.DuckDBOpts.Threads, "duckdb-threads", 20, "DuckDB threads")
	fs.IntVar(&o.DuckDBOpts.BatchSize, "duckdb-batch", 100000, "DuckDB batch")
}

func (o *SyncOptions) Validate() error {
	if o.SourceBucket == "" && o.RecordFile == "" && o.Mode != known.PreprocessModel {
		return errors.New("source-bucket is required")
	}
	if o.TargetBucket == "" && o.RecordFile == "" && o.Mode != known.PreprocessModel {
		return errors.New("target-bucket is required")
	}
	if o.TargetBucket == "" && o.SourceBucket == "" && o.RecordFile == "" && o.Mode != known.PreprocessModel {
		return errors.New("if target-bucket and source-bucket is not set, record-file is required")
	}
	if o.RecordFile != "" {
		if _, err := os.Stat(o.RecordFile); os.IsNotExist(err) {
			return errors.New("record-file: " + o.RecordFile + " not found")
		}
		if o.TargetBucket == "" || o.SourceBucket == "" {
			return errors.New("target-bucket and source-bucket are required when record-file is set")
		}
	}
	return nil
}
