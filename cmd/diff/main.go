package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: program file1 file2 output")
		return
	}

	file1Path := os.Args[1]
	file2Path := os.Args[2]
	outputPath := os.Args[3]

	// 使用 map 存储文件1的内容
	content1 := make(map[string]struct{})

	// 使用 WaitGroup 来协调goroutine
	var wg sync.WaitGroup
	wg.Add(1)

	// 并发读取第一个文件
	go func() {
		defer wg.Done()
		file1, err := os.Open(file1Path)
		if err != nil {
			fmt.Printf("Error opening file1: %v\n", err)
			return
		}
		defer file1.Close()

		scanner := bufio.NewScanner(file1)
		for scanner.Scan() {
			content1[scanner.Text()] = struct{}{}
		}
	}()

	// 打开输出文件
	output, err := os.Create(outputPath)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer output.Close()

	writer := bufio.NewWriter(output)
	defer writer.Flush()

	// 等待第一个文件读取完成
	wg.Wait()

	// 读取第二个文件并比较
	file2, err := os.Open(file2Path)
	if err != nil {
		fmt.Printf("Error opening file2: %v\n", err)
		return
	}
	defer file2.Close()

	scanner := bufio.NewScanner(file2)
	for scanner.Scan() {
		line := scanner.Text()
		// 如果该行在file1中不存在，则写入输出文件
		if _, exists := content1[line]; !exists {
			writer.WriteString(line + "\n")
		}
	}
}
