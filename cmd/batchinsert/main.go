package main

import (
	"DataImport/internal/db/postgresql"
	_ "DataImport/internal/pkg/config"
	"DataImport/internal/pkg/utils"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	BatchSize   = 3000
	WorkerCount = 6
	MaxRecords  = 12000000
)

type Message struct {
	data     []postgresql.Record
	filename string
}

func main() {
	start := time.Now()
	err := postgresql.InitDB()
	if err != nil {
		fmt.Printf("init postgresql failed: %v", err)
		panic(err)
	}

	workdir := os.Args[1]
	jobs := make(chan Message, WorkerCount)

	var successCount int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go consumer(wg, jobs, i, &successCount, ctx, cancel)
	}

	// 启动producer 不用添加wg.Add,因为consumer会等待到close(jobs)操作才会停止
	go func() {
		producer(workdir, jobs, ctx)
		close(jobs)
	}()

	wg.Wait()
	elapsed := time.Since(start) // 计算耗时
	fmt.Printf("程序总共耗时: %v\n", elapsed)
}

func producer(workdir string, jobs chan<- Message, ctx context.Context) {
	// 扫描文件总数
	files, err := filepath.Glob(workdir + "/*.jsonl")
	if err != nil {
		panic(err)
	}
	fmt.Printf("扫描到的文件总数: %d\n", len(files))
	for _, filepath := range files {
		select {
		case <-ctx.Done():
			return
		default:
			readFile(filepath, jobs)
		}

	}
}

func consumer(wg *sync.WaitGroup, jobs <-chan Message, idx int, successCount *int64, ctx context.Context, cancel context.CancelFunc) {
	defer wg.Done()
	for message := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batch := message.data
		filename := message.filename
		//err := postgresql.BatchInsert(batch)
		//err := postgresql.BatchInsertWithParams(batch)
		// 检查当前已成功插入的数量
		currentCount := atomic.LoadInt64(successCount)
		if currentCount >= MaxRecords {
			fmt.Printf("Consumer_%d: 已达到目标，跳过本批次\n", idx)
			continue
		}

		// 计算还需要插入多少条
		remaining := MaxRecords - currentCount
		needInsert := batch

		// 如果本批次会超出限制，只取需要的部分
		if int64(len(batch)) > remaining {
			needInsert = batch[:remaining]
			fmt.Printf("Consumer_%d: 调整批次大小 %d -> %d (还需要%d条)\n",
				idx, len(batch), len(needInsert), remaining)
		}

		err := postgresql.BatchCopyInsert(needInsert)
		if err != nil {
			fmt.Println("批量插入时出错:", err)
			continue
		}
		newCount := atomic.AddInt64(successCount, int64(len(needInsert)))
		fmt.Printf("work_%d,文件:%s 插入:%d条数据\n", idx, filename, len(needInsert))
		if newCount >= MaxRecords {
			fmt.Printf("Consumer_%d: 已成功插入 %d 条，触发停止信号\n", idx, newCount)
			cancel()
			return
		}
	}
}

func readFile(filepath string, jobs chan<- Message) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("打开文件时出错:", err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	maxLineLength := 1024 * 1024 * 24
	scanner.Buffer(make([]byte, 0, maxLineLength), maxLineLength)
	results := make([]postgresql.Record, 0, BatchSize)
	for scanner.Scan() {
		// 发送到channel
		if len(results) == BatchSize {
			jobs <- Message{data: results, filename: filepath}
			results = make([]postgresql.Record, 0, BatchSize)
		}

		lineBytes := scanner.Bytes()
		lineBytes = bytes.ToValidUTF8(lineBytes, nil)

		var record postgresql.Record
		if err := json.Unmarshal(lineBytes, &record); err != nil {
			fmt.Println("解析 JSON 时出错:", err)
			continue
		}
		updateDefaultMsg(&record)
		if record.Content == "" || record.Title == "" {
			continue
		}
		results = append(results, record)
	}
	// 发送最后一批数据
	if len(results) > 0 {
		jobs <- Message{data: results, filename: filepath}
	}

	// 检查是否有扫描错误
	if err := scanner.Err(); err != nil {
		fmt.Println("扫描文件时出错:", err)
		return
	}
	fmt.Println("文件读取完成:", filepath)
	utils.WriteToFile(filepath)
}

func updateDefaultMsg(record *postgresql.Record) {
	record.CreatedAt = time.Now()
	record.UpdatedAt = time.Now()
	record.AccountId = 1
	if record.Content == "" {
		record.Content = record.Text
	}
	setTitle(record)
	record.Title = string(bytes.ToValidUTF8([]byte(record.Title), []byte{}))
	record.Content = string(bytes.ToValidUTF8([]byte(record.Content), []byte{}))
	record.Title = strings.ReplaceAll(record.Title, "\x00", "")
	record.Content = strings.ReplaceAll(record.Content, "\x00", "")
}

func setTitle(record *postgresql.Record) {
	content := record.Content
	record.Title = string([]rune(content)[:15])
}
