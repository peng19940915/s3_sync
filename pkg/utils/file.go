package utils

import (
	"bufio"
	"bytes"
	"io"
	"os"
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
