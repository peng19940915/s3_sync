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
	S3CopyMaxRetries = 3
	S3MaxCopyLimit   = 2000
	S3CopyMaxWait    = 30
)

const (
	FileBufferSize = 1 * 1024 * 1024 // 1MB
)
