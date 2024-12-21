package utils

import (
	"bufio"
	"bytes"
	"io"
	"net/url"
	"os"
	"strings"
)

func CountFileLines(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	buf := make([]byte, 8*1024*1024)
	count := 0
	lineSep := []byte{'\n'}

	reader := bufio.NewReaderSize(file, 8*1024*1024)

	for {
		c, err := reader.Read(buf)
		if c > 0 {
			count += bytes.Count(buf[:c], lineSep)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
	}

	return count, nil
}

// URLEncode encodes the string for use in URLs while preserving forward slashes
func URLEncode(s string) string {
	parts := strings.Split(s, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}
