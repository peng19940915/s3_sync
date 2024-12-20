package app

import (
	"context"
	"flag"
	"fmt"

	"github.com/peng19940915/s3_sync/pkg/known"
	"github.com/peng19940915/s3_sync/pkg/options"
	"github.com/peng19940915/s3_sync/pkg/syncer"
	"github.com/spf13/cobra"
)

func NewCommand(ctx context.Context) *cobra.Command {
	opts := options.NewSyncOptions()
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync files between S3 buckets",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.ShowVersion {
				fmt.Println(known.Get().Pretty())
				return nil
			}
			if err := opts.Validate(); err != nil {
				return err
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
	syncer := syncer.NewSyncer(opts)
	return syncer.Run(ctx)
}
