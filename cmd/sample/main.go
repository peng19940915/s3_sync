package main
/*
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build .
*/
import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

// 水塘抽样结构体
type Reservoir struct {
	samples []string
	count   int
	size    int
	rnd     *rand.Rand
}

func NewReservoir(size int) *Reservoir {
	return &Reservoir{
		samples: make([]string, size),
		count:   0,
		size:    size,
		rnd:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (r *Reservoir) Add(item string) {
	r.count++
	// 如果计数小于目标大小，直接添加
	if r.count <= r.size {
		r.samples[r.count-1] = item
		return
	}
	// 否则以 size/count 的概率替换现有样本
	j := r.rnd.Intn(r.count)
	if j < r.size {
		r.samples[j] = item
	}
}

func (r *Reservoir) GetSamples() []string {
	if r.count < r.size {
		return r.samples[:r.count]
	}
	return r.samples
}

func main() {
	// 定义命令行参数
	inputPath := flag.String("input", "input.txt", "输入文件路径")
	outputPath := flag.String("output", "output.txt", "输出文件路径")
	sampleSize := flag.Int("size", 1000, "每个日期的抽样数量")
	whiteListFilePath := flag.String("whiteList", "whiteList.txt", "白名单文件路径")

	// 解析命令行参数
	flag.Parse()

	// 验证参数
	if *inputPath == "" {
		fmt.Println("错误：必须指定输入文件路径")
		flag.Usage()
		return
	}
	// 读取白名单
	whiteList := make(map[string]struct{})
	wlFile, err := os.Open(*whiteListFilePath)
	if err != nil {
		fmt.Printf("无法打开白名单文件: %v\n", err)
		return
	}
	defer wlFile.Close()
	whiteListScanner := bufio.NewScanner(wlFile)
	for whiteListScanner.Scan() {
		whiteList[whiteListScanner.Text()] = struct{}{}
	}
	fmt.Printf("已加载白名单，共 %d 条记录\n", len(whiteList))
	// 创建一个map来存储每个日期对应的水塘抽样器
	reservoirs := make(map[string]*Reservoir)

	// 打开输入文件
	file, err := os.Open(*inputPath)
	if err != nil {
		fmt.Printf("无法打开文件: %v\n", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// 增加扫描器的缓冲区大小，但不预先分配大内存
	scanner.Buffer(nil, 1024*1024)

	// 单次遍历文件
	for scanner.Scan() {
		line := scanner.Text()
		// 检查是否在白名单中
		if _, isWhiteListed := whiteList[line]; isWhiteListed {
			continue
		}
		date := strings.Split(line, "/")[0]

		// 如果这个日期还没有对应的水塘，创建一个
		if _, exists := reservoirs[date]; !exists {
			reservoirs[date] = NewReservoir(*sampleSize)
		}
		// 将当前行添加到对应日期的水塘中
		reservoirs[date].Add(line)
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("读取文件时发生错误: %v\n", err)
		return
	}

	// 创建输出文件
	outFile, err := os.Create(*outputPath)
	if err != nil {
		fmt.Printf("创建输出文件失败: %v\n", err)
		return
	}
	defer outFile.Close()
	writer := bufio.NewWriter(outFile)
	defer writer.Flush()
	// 写入白名单
	for line, _ := range whiteList {
		writer.WriteString(line + "\n")
	}
	// 写入所有抽样结果
	for date, reservoir := range reservoirs {
		for _, sample := range reservoir.GetSamples() {
			writer.WriteString(sample + "\n")
		}
		fmt.Printf("已处理 %s: 总数 %d, 抽样 %d 条数据\n",
			date, reservoir.count, len(reservoir.GetSamples()))
	}
}
