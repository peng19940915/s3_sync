package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	aws_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func NewClient(region string) *aws_s3.Client {
	// 加载默认 AWS 配置
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		panic(err)
	}

	// 创建 S3 客户端
	return aws_s3.NewFromConfig(cfg)
}
