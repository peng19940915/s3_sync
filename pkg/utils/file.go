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

const (
	BufferSize = 1000
	BatchSize  = 10000
)

// func ShuffleFile(filename string) error {
// 	newFilename := filename + ".shuffled"
// 	input, err := os.Open(filename)
// 	if err != nil {
// 		return err
// 	}
// 	defer input.Close()

// 	output, err := os.Create(newFilename)
// 	if err != nil {
// 		return err
// 	}
// 	defer output.Close()

// 	// 创建channel用于传输数据
// 	pathChan := make(chan string, BufferSize)

// 	// 启动写入goroutine
// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		writer := bufio.NewWriter(output)
// 		defer writer.Flush()
// 		buffers := make(map[string][]string)
// 		count := 0
// 		// 处理一批数据
// 		processBatch := func() {
// 			for len(buffers) > 0 {
// 				for prefix, paths := range buffers {
// 					if len(paths) > 0 {
// 						fmt.Fprintln(writer, paths[0])
// 						buffers[prefix] = paths[1:]
// 						if len(buffers[prefix]) == 0 {
// 							delete(buffers, prefix)
// 						}
// 					}
// 				}
// 			}
// 		}
// 		// 持续处理输入
// 		for path := range pathChan {
// 			prefix := filepath.Dir(path)
// 			buffers[prefix] = append(buffers[prefix], path)
// 			count++

// 			if count >= BatchSize {
// 				processBatch()
// 				count = 0
// 			}
// 		}

// 		// 处理剩余数据
// 		processBatch()
// 	}()
// 	// 读取文件
// 	scanner := bufio.NewScanner(input)
// 	for scanner.Scan() {
// 		pathChan <- scanner.Text()
// 	}

// 	// 关闭channel并等待处理完成
// 	close(pathChan)
// 	wg.Wait()
// 	return nil
// }
