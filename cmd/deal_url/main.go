package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	var inputFile string
	flag.StringVar(&inputFile, "f", "", "input file path")
	flag.Parse()

	if inputFile == "" {
		fmt.Println("Please specify input file with -f")
		return
	}

	// 创建输出文件名
	dir := filepath.Dir(inputFile)
	base := filepath.Base(inputFile)
	ext := filepath.Ext(base)
	nameWithoutExt := strings.TrimSuffix(base, ext)

	normalFile := filepath.Join(dir, nameWithoutExt+"_normal"+ext)
	urlEncodedFile := filepath.Join(dir, nameWithoutExt+"_urlencoded"+ext)

	// 打开输入文件
	input, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("Failed to open input file: %v\n", err)
		return
	}
	defer input.Close()

	// 创建输出文件
	normal, err := os.Create(normalFile)
	if err != nil {
		fmt.Printf("Failed to create normal file: %v\n", err)
		return
	}
	defer normal.Close()

	urlEncoded, err := os.Create(urlEncodedFile)
	if err != nil {
		fmt.Printf("Failed to create urlencoded file: %v\n", err)
		return
	}
	defer urlEncoded.Close()

	// 创建writers
	normalWriter := bufio.NewWriter(normal)
	urlEncodedWriter := bufio.NewWriter(urlEncoded)
	defer normalWriter.Flush()
	defer urlEncodedWriter.Flush()

	// 读取并处理文件
	scanner := bufio.NewScanner(input)
	var normalCount, urlEncodedCount int

	for scanner.Scan() {
		line := scanner.Text()
		decoded, err := url.PathUnescape(line)
		if err == nil && decoded != line {
			// URL编码的行
			urlEncodedWriter.WriteString(line + "\n")
			urlEncodedCount++
		} else {
			// 普通行
			normalWriter.WriteString(line + "\n")
			normalCount++
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}

	fmt.Printf("Processing complete:\n")
	fmt.Printf("Normal lines: %d -> %s\n", normalCount, normalFile)
	fmt.Printf("URL-encoded lines: %d -> %s\n", urlEncodedCount, urlEncodedFile)
}
