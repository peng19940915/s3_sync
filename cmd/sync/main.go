package main

import (
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/peng19940915/s3_sync/cmd/sync/app"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

func main() {
	ctx := signals.SetupSignalHandler()
	if err := app.NewCommand(ctx).Execute(); err != nil {
		hlog.Fatal(err)
	}
}
