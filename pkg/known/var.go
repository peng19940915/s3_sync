package known

import (
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

var DefaultBackoff = wait.Backoff{
	Duration: time.Second,                 // 初始等待时间是 1 秒
	Factor:   2.0,                         // 每次重试后等待时间都会乘以 2
	Jitter:   0.1,                         // 添加 10% 的随机波动，避免多个请求同时重试
	Steps:    S3CopyMaxRetries,            // 最多重试 5 次
	Cap:      S3CopyMaxWait * time.Second, // 最大等待时间不超过 30 秒
}
