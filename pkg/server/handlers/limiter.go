package handlers

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
)

func LimiterShardDetail(ctx context.Context, c *app.RequestContext) {
	c.String(200, "ok") //syncer.PathLimiter.GetShardDetail())
	return
}
