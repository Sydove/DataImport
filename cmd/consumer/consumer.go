package main

import (
	"DataImport/internal/db/postgresql"
	_ "DataImport/internal/pkg/config"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

/*
1.扫描文件总数
2.遍历文件，读取文件内容
3.解析文件内容，转换为数据库模型
4.写入数据库
*/

const (
	BatchSize   = 2000
	WorkerCount = 4
)

type Message struct {
	data     []postgresql.Record
	filename string
}

func main() {

	err := postgresql.InitDB()
	if err != nil {
		fmt.Printf("init postgresql failed: %v", err)
		panic(err)
	}

	workdir := os.Args[1]
	jobs := make(chan Message, BatchSize)

	wg := &sync.WaitGroup{}
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go consumer(wg, jobs, i)
	}

	// 启动producer
	go func() {
		producer(workdir, jobs)
		close(jobs)
	}()

	wg.Wait()
}

func producer(workdir string, jobs chan<- Message) {
	// 扫描文件总数
	files, err := filepath.Glob(workdir + "/*.jsonl")
	if err != nil {
		panic(err)
	}
	fmt.Println(files)
	for _, filepath := range files {
		readFile(filepath, jobs)
	}
}

func consumer(wg *sync.WaitGroup, jobs <-chan Message, idx int) {
	defer wg.Done()
	for message := range jobs {
		batch := message.data
		filename := message.filename
		err := postgresql.BatchInsert(batch)
		if err != nil {
			fmt.Println("批量插入时出错:", err)
		}
		fmt.Printf("work_%d,文件:%s 插入:%d条数据\n", idx, filename, len(batch))
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
	maxLineLength := 1024 * 1024 * 1024
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
}

func updateDefaultMsg(record *postgresql.Record) {
	record.CreatedAt = time.Now()
	record.UpdatedAt = time.Now()
	record.AccountId = 1
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
