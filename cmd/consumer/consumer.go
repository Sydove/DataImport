import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"DataImport/internal/db/postgresql"
)

// -------------------- 辅助函数 --------------------

// 递归遍历结构体/切片/map 等，清理所有 string 字段：
// - bytes.ToValidUTF8 -> 修复非法 UTF-8 字节
// - 删除 NUL 字节 '\x00'
// 注意：只会修改可设置的导出字段（常见场景足够）
func sanitizeStructStrings(v any) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return
	}
	rv = rv.Elem()
	sanitizeValue(rv)
}

func sanitizeValue(v reflect.Value) {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		if !v.IsNil() {
			sanitizeValue(v.Elem())
		}
	case reflect.String:
		if v.CanSet() {
			s := v.String()
			// 去掉非法 UTF-8 字节（高效）
			s = string(bytes.ToValidUTF8([]byte(s), nil))
			// 去除 NUL 字节（Postgres 不接受）
			if strings.IndexByte(s, 0) != -1 {
				s = strings.ReplaceAll(s, "\x00", "")
			}
			v.SetString(s)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			// 递归，但只处理导出字段（未导出字段不可设值）
			// 即便不可设值，我们仍然递归查看其内部（以处理嵌套可导出字段）
			sanitizeValue(f)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			sanitizeValue(v.Index(i))
		}
	case reflect.Map:
		for _, key := range v.MapKeys() {
			sanitizeValue(v.MapIndex(key))
		}
	}
}

// 安全生成标题：取 content 前 N 个 rune（考虑长度不足）
func setTitle(record *postgresql.Record) {
	const titleRunes = 15
	runes := []rune(record.Content)
	if len(runes) > titleRunes {
		record.Title = string(runes[:titleRunes])
	} else {
		record.Title = string(runes)
	}
}

// 将记录统一处理：时间、默认值、字段清理
func updateDefaultMsg(record *postgresql.Record) {
	now := time.Now()
	record.CreatedAt = now
	record.UpdatedAt = now
	record.AccountId = 1

	// 先对所有 string 字段做通用清理（包括 Title/Content/其它）
	sanitizeStructStrings(record)

	// 再产生 Title（因为 Title 可能依赖 Content）
	setTitle(record)

	// 再一次确保 Title/Content 没有非法 UTF-8 或 NUL（保险）
	record.Title = string(bytes.ToValidUTF8([]byte(record.Title), nil))
	record.Title = strings.ReplaceAll(record.Title, "\x00", "")
	record.Content = string(bytes.ToValidUTF8([]byte(record.Content), nil))
	record.Content = strings.ReplaceAll(record.Content, "\x00", "")
}

// -------------------- 文件读取/生产者（改进） --------------------

func readFile(filepath string, jobs chan<- []postgresql.Record) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("打开文件时出错:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	maxLineLength := 1024 * 1024 * 4
	scanner.Buffer(make([]byte, 0, maxLineLength), maxLineLength)

	results := make([]postgresql.Record, 0, BatchSize)
	for scanner.Scan() {
		// 读取原始字节并先做整体 UTF-8 修复（去掉明显破损）
		lineBytes := scanner.Bytes()
		lineBytes = bytes.ToValidUTF8(lineBytes, nil)

		var record postgresql.Record
		if err := json.Unmarshal(lineBytes, &record); err != nil {
			// 如果 JSON 解析失败，打印并跳过（可选：记录到错误日志文件）
			fmt.Println("解析 JSON 时出错:", err)
			continue
		}

		// 对结构体内各 string 字段做逐字段清理，设置默认值，生成标题
		updateDefaultMsg(&record)

		results = append(results, record)

		// 达到批次大小：发送一份**拷贝**到 channel，重用 results 底层内存
		if len(results) >= BatchSize {
			batch := make([]postgresql.Record, len(results))
			copy(batch, results)
			jobs <- batch
			results = results[:0] // 复用底层数组，避免频繁分配
		}
	}

	// 发送剩余的（如果有）
	if len(results) > 0 {
		batch := make([]postgresql.Record, len(results))
		copy(batch, results)
		jobs <- batch
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("扫描文件时出错:", err)
		return
	}
	fmt.Println("文件读取完成:", filepath)
}
