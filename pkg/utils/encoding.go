package utils

import "net/url"

// EncodeNonASCII 对字符串中的非ASCII字符（如中文）进行URL编码，保持ASCII字符不变
func EncodeNonASCII(s string) string {
	encoded := ""
	for _, r := range s {
		if r > 127 { // 非 ASCII 字符
			encoded += url.QueryEscape(string(r))
		} else { // ASCII 字符保持原样
			encoded += string(r)
		}
	}
	return encoded
}
