package handlers

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/peng19940915/s3_sync/pkg/syncer"
)

func LimiterShardDetail(ctx context.Context, c *app.RequestContext) {
	c.String(200, syncer.PathLimiter.GetShardDetail())
	return
}
