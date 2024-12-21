package options

import (
	"errors"
	"os"

	"github.com/spf13/pflag"
)

type SyncOptions struct {
	SourceBucket string
	TargetBucket string
	ShowVersion  bool
	Region       string
	Workers      int
	Prefix       string
	RecordFile   string
}

func NewSyncOptions() *SyncOptions {
	return &SyncOptions{}
}

func (o *SyncOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVarP(&o.SourceBucket, "source-bucket", "s", "null", "Source bucket")
	fs.StringVarP(&o.TargetBucket, "target-bucket", "t", "null", "Target bucket")
	fs.BoolVarP(&o.ShowVersion, "version", "v", false, "Show version")
	fs.StringVarP(&o.Region, "region", "r", "us-east-1", "Region")
	fs.IntVarP(&o.Workers, "workers", "w", 10, "Workers")
	fs.StringVarP(&o.Prefix, "prefix", "p", "", "Prefix")
	fs.StringVarP(&o.RecordFile, "record-file", "f", "", "Record file")
}

func (o *SyncOptions) Validate() error {
	if o.SourceBucket == "" && o.RecordFile == "" {
		return errors.New("source-bucket is required")
	}
	if o.TargetBucket == "" && o.RecordFile == "" {
		return errors.New("target-bucket is required")
	}
	if o.TargetBucket == "" && o.SourceBucket == "" && o.RecordFile == "" {
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
