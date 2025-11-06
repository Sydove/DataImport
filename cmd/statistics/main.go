package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FileResult 存储单个文件的统计结果
type FileResult struct {
	Path         string
	RecordCount  int
	InvalidLines int
	Error        error
}

// Config 配置选项
type Config struct {
	Dir          string
	Verbose      bool
	Validate     bool
	Parallel     bool
	ShowProgress bool
}

func main() {
	// 命令行参数
	var config Config
	flag.StringVar(&config.Dir, "dir", "", "要统计的目录路径（必需）")
	flag.BoolVar(&config.Verbose, "verbose", false, "显示每个文件的详细信息")
	flag.BoolVar(&config.Validate, "validate", false, "验证JSON格式（较慢）")
	flag.BoolVar(&config.Parallel, "parallel", true, "并行处理文件")
	flag.BoolVar(&config.ShowProgress, "progress", false, "显示处理进度")
	flag.Parse()

	if config.Dir == "" {
		fmt.Println("错误: 请提供目录路径")
		fmt.Println("\n用法:")
		flag.PrintDefaults()
		fmt.Println("\n示例:")
		fmt.Println("  ./jsonl_counter -dir /path/to/jsonl/files")
		fmt.Println("  ./jsonl_counter -dir ./data -verbose")
		fmt.Println("  ./jsonl_counter -dir ./data -validate -verbose")
		os.Exit(1)
	}

	// 检查目录是否存在
	if _, err := os.Stat(config.Dir); os.IsNotExist(err) {
		fmt.Printf("错误: 目录 '%s' 不存在\n", config.Dir)
		os.Exit(1)
	}

	fmt.Printf("开始统计目录: %s\n", config.Dir)
	fmt.Println("正在扫描文件...")
	fmt.Println()

	startTime := time.Now()

	// 查找所有 .jsonl 文件
	files, err := findJSONLFiles(config.Dir)
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		os.Exit(1)
	}

	if len(files) == 0 {
		fmt.Println("未找到 .jsonl 文件")
		os.Exit(0)
	}

	// 统计文件
	var results []FileResult
	if config.Parallel {
		results = processFilesParallel(files, &config)
	} else {
		results = processFilesSequential(files, &config)
	}

	// 汇总结果
	printResults(results, &config, time.Since(startTime))
}

// findJSONLFiles 查找目录下所有 .jsonl 文件
func findJSONLFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".jsonl") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// processFilesSequential 顺序处理文件
func processFilesSequential(files []string, config *Config) []FileResult {
	results := make([]FileResult, 0, len(files))
	for i, file := range files {
		if config.ShowProgress {
			fmt.Printf("\r处理进度: %d/%d", i+1, len(files))
		}
		result := countRecords(file, config.Validate)
		results = append(results, result)

		if config.Verbose && result.Error == nil {
			fmt.Printf("\n文件: %s\n", result.Path)
			fmt.Printf("  记录数: %d\n", result.RecordCount)
			if config.Validate && result.InvalidLines > 0 {
				fmt.Printf("  无效行: %d\n", result.InvalidLines)
			}
		}
	}
	if config.ShowProgress {
		fmt.Println()
	}
	return results
}

// processFilesParallel 并行处理文件
func processFilesParallel(files []string, config *Config) []FileResult {
	var wg sync.WaitGroup
	resultChan := make(chan FileResult, len(files))
	semaphore := make(chan struct{}, 10) // 限制并发数

	for _, file := range files {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			semaphore <- struct{}{}        // 获取信号量
			defer func() { <-semaphore }() // 释放信号量

			result := countRecords(f, config.Validate)
			resultChan <- result

			if config.Verbose && result.Error == nil {
				fmt.Printf("文件: %s, 记录数: %d\n", result.Path, result.RecordCount)
			}
		}(file)
	}

	wg.Wait()
	close(resultChan)

	results := make([]FileResult, 0, len(files))
	for result := range resultChan {
		results = append(results, result)
	}
	return results
}

// countRecords 统计单个文件的记录数
func countRecords(filename string, validate bool) FileResult {
	result := FileResult{Path: filename}

	file, err := os.Open(filename)
	if err != nil {
		result.Error = err
		return result
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	lineNum := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			result.Error = err
			return result
		}

		// 去除空白字符
		line = strings.TrimSpace(line)

		// 跳过空行
		if line != "" {
			lineNum++
			if validate {
				// 验证JSON格式
				var js json.RawMessage
				if err := json.Unmarshal([]byte(line), &js); err != nil {
					result.InvalidLines++
				} else {
					result.RecordCount++
				}
			} else {
				result.RecordCount++
			}
		}

		if err == io.EOF {
			break
		}
	}

	return result
}

// printResults 打印统计结果
func printResults(results []FileResult, config *Config, duration time.Duration) {
	totalRecords := 0
	totalInvalid := 0
	fileCount := 0
	errorCount := 0

	for _, result := range results {
		if result.Error != nil {
			errorCount++
			if config.Verbose {
				fmt.Printf("错误: %s - %v\n", result.Path, result.Error)
			}
			continue
		}
		fileCount++
		totalRecords += result.RecordCount
		totalInvalid += result.InvalidLines
	}

	fmt.Println("\n========================================")
	fmt.Println("           统计结果")
	fmt.Println("========================================")
	fmt.Printf("文件数量:       %d\n", fileCount)
	fmt.Printf("JSON记录总数:   %d\n", totalRecords)
	if config.Validate {
		fmt.Printf("无效记录总数:   %d\n", totalInvalid)
		if totalRecords+totalInvalid > 0 {
			validPercent := float64(totalRecords) / float64(totalRecords+totalInvalid) * 100
			fmt.Printf("有效率:         %.2f%%\n", validPercent)
		}
	}
	if errorCount > 0 {
		fmt.Printf("错误文件数:     %d\n", errorCount)
	}
	if fileCount > 0 {
		avgRecords := totalRecords / fileCount
		fmt.Printf("平均记录数:     %d 条/文件\n", avgRecords)
	}
	fmt.Printf("处理耗时:       %v\n", duration)
	fmt.Println("========================================")
}
