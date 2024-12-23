package app

import (
	"context"
	"flag"
	"fmt"

	datapreprocess "github.com/peng19940915/s3_sync/pkg/data_preprocess"
	"github.com/peng19940915/s3_sync/pkg/known"
	"github.com/peng19940915/s3_sync/pkg/options"
	"github.com/peng19940915/s3_sync/pkg/server"
	"github.com/peng19940915/s3_sync/pkg/syncer"
	"github.com/spf13/cobra"
)

func NewCommand(ctx context.Context) *cobra.Command {
	opts := options.NewSyncOptions()
	cmd := &cobra.Command{
		Use:           "sync",
		Short:         "Sync files/S3 between S3 buckets",
		SilenceUsage:  true, // 添加这一行，禁止在发生错误时打印使用说明
		SilenceErrors: true, // 添加这行来禁止错误信息的打印
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.ShowVersion {
				fmt.Println(known.Get().Pretty())
				return nil
			}
			if err := opts.Validate(); err != nil {
				return err
			}
			if opts.Mode == known.PreprocessModel {
				return datapreprocess.ProcessS3Files(ctx, opts.DataPreprocessOptions.Bucket, opts.DataPreprocessOptions.Prefix, opts.DataPreprocessOptions.OutputFile)
			}
			return Run(ctx, opts)
		},
	}

	// 添加所有标志
	cmd.Flags().AddGoFlagSet(flag.CommandLine)
	opts.AddFlags(cmd.Flags())
	return cmd
}

func Run(ctx context.Context, opts *options.SyncOptions) error {
	syncer := syncer.NewSyncer(ctx, opts)
	server := server.NewServer(&opts.ServerOpts)
	go server.Run(ctx, &opts.ServerOpts)
	return syncer.Run(ctx, opts)
}
