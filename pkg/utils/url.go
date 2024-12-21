package utils

import (
	"net/url"
	"strings"
)

// URLEncode encodes the string for use in URLs while preserving forward slashes
func URLEncode(s string) string {
	parts := strings.Split(s, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}
