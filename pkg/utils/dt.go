package utils

import (
	"net/url"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/peng19940915/s3_sync/pkg/known"
)

func CheckDt(path string, start, end time.Time) bool {
	if start.IsZero() || end.IsZero() {
		return true
	}
	decodedPath, err := url.QueryUnescape(path)
	if err != nil {
		hlog.Errorf("failed to unescape path: %v", err)
		return false
	}
	matches := known.DtRegex.FindStringSubmatch(decodedPath)
	// 如果匹配不到dt，则返回false
	if len(matches) < 2 {
		return false
	}
	currentDt, err := time.Parse("2006-01-02", matches[1])
	if err != nil {
		//hlog.Errorf("failed to parse dt: %v", err)
		return false
	}
	if (currentDt.After(start) || currentDt.Equal(start)) && (currentDt.Before(end) || currentDt.Equal(end)) {
		return true
	}
	return false
}
