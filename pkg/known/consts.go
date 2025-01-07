package known

const (
	SuccessSyncStatus = "success"
	FailedSyncStatus  = "failed"
)

const (
	FailedRecordPath  = "failed_record"
	SuccessRecordPath = "success_record"
)

const (
	S3CopyMaxRetries           = 3    // 调用S3Copy 重试次数
	S3MaxCopyLimit             = 2500 // 调用 S3Copy Limit
	S3CopyMaxWait              = 30   // 最大等待时间
	S3PathShardCount           = 256  // 一共多少分片
	S3PathShardExipireTime     = 120  // seconds
	S3PathShardCleanUpInterval = 30   // seconds
	S3PathShardMask            = S3PathShardCount - 1
)

const (
	FileBufferSize = 1 * 1024 * 1024 // 1MB
)

const (
	PreprocessModel = "preprocess"
	SyncModel       = "sync"
)

const (
	PreProcessMaxWorkersForS3 = 50 // 并发从S3下载数据
	PreProcessBatchSize       = 500000
)
//200->400->800->1600->3200
