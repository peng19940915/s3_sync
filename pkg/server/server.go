package server

import (
	"context"
	"fmt"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/hertz-contrib/pprof"
	"github.com/peng19940915/s3_sync/pkg/options"
	"github.com/peng19940915/s3_sync/pkg/server/handlers"
)

type Server struct {
	*server.Hertz
	opts options.ServerOptions
}

func NewServer(opts *options.ServerOptions) *Server {
	h := &Server{
		Hertz: server.Default(server.WithHostPorts(fmt.Sprintf(":%d", opts.BindPort))),
		opts:  *opts,
	}
	return h
}

func (s *Server) Run(ctx context.Context, opts *options.ServerOptions) {
	s.GET("/limiterShardDetail", handlers.LimiterShardDetail)
	if opts.PprofEnable {
		pprof.Register(s.Hertz, "/debug")
	}

	go func() {
		if err := s.Hertz.Run(); err != nil {
			fmt.Printf("server run error: %v\n", err)
		}
	}()

	<-ctx.Done()

	// 优雅关闭服务器
	if err := s.Hertz.Shutdown(context.Background()); err != nil {
		hlog.Errorf("server shutdown error: %v\n", err)
	} else {
		hlog.Info("server shutdown success")
	}
}
