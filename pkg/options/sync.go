package options

import (
	"errors"

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
	fs.StringVarP(&o.SourceBucket, "source-bucket", "s", "", "Source bucket")
	fs.StringVarP(&o.TargetBucket, "target-bucket", "t", "", "Target bucket")
	fs.BoolVarP(&o.ShowVersion, "version", "v", false, "Show version")
	fs.StringVarP(&o.Region, "region", "r", "us-east-1", "Region")
	fs.IntVarP(&o.Workers, "workers", "w", 10, "Workers")
	fs.StringVarP(&o.Prefix, "prefix", "p", "", "Prefix")
	fs.StringVarP(&o.RecordFile, "record-file", "f", "", "Record file")
}

func (o *SyncOptions) Validate() error {
	if o.SourceBucket == "" {
		return errors.New("source-bucket is required")
	}
	if o.TargetBucket == "" {
		return errors.New("target-bucket is required")
	}
	if o.Prefix != "" {
		return errors.New("prefix is required")
	}
	return nil
}
