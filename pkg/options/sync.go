package options

import (
	"errors"
	"os"
	"time"

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
	//DuckDBOpts            DuckDBOptions
	StartDt string
	EndDt   string
}

type ServerOptions struct {
	BindPort    int
	PprofEnable bool
}

type DataPreprocessOptions struct {
	Bucket        string
	FileBatchSize int
	OutputFile    string
	Prefix        string
	ManifestFile  string
}

/*
type DuckDBOptions struct {
	MemLimit  string
	DBPath    string
	Threads   int
	BatchSize int
}
*/

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
	fs.StringVar(&o.StartDt, "start-dt", "", "Start dt")
	fs.StringVar(&o.EndDt, "end-dt", "", "End dt")
	// data preprocess for storage inventroy configuration
	fs.StringVar(&o.DataPreprocessOptions.Bucket, "inventory-bucket", "", "Input Bucket")
	fs.StringVar(&o.DataPreprocessOptions.OutputFile, "inventory-output-file", "", "Output file")
	fs.StringVar(&o.DataPreprocessOptions.Prefix, "inventory-prefix", "", "Storage inventory configuration prefix")
	fs.IntVar(&o.DataPreprocessOptions.FileBatchSize, "inventory-file-batch-size", 5000000, "Storage inventory file batch size")
	fs.StringVar(&o.DataPreprocessOptions.ManifestFile, "inventory-manifest-file", "manifest.json", "Storage inventory manifest file")
	// duckdb
	//fs.StringVar(&o.DuckDBOpts.MemLimit, "duckdb-mem-limit", "8GB", "DuckDB mem limit")
	//fs.StringVar(&o.DuckDBOpts.DBPath, "duckdb-db-path", "duckdb.db", "DuckDB db path")
	//fs.IntVar(&o.DuckDBOpts.Threads, "duckdb-threads", 20, "DuckDB threads")
	//fs.IntVar(&o.DuckDBOpts.BatchSize, "duckdb-batch", 100000, "DuckDB batch")
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
	if o.StartDt != "" || o.EndDt != "" {
		if o.StartDt == "" || o.EndDt == "" {
			return errors.New("start-dt and end-dt are required when start-dt or end-dt is set")
		}
		start, err := time.Parse("2006-01-02", o.StartDt)
		if err != nil {
			return errors.New("failed to parse start-dt: " + err.Error())
		}
		end, err := time.Parse("2006-01-02", o.EndDt)
		if err != nil {
			return errors.New("failed to parse end-dt: " + err.Error())
		}
		if start.After(end) {
			return errors.New("start-dt is after end-dt: " + o.StartDt + ", " + o.EndDt)
		}
	}
	return nil
}
