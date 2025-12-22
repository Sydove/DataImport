package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func GetConfigPath() string {
	var configPath string
	configPath = os.Getenv("CONFIG_PATH")
	if configPath == "" {
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			panic("cannot get project root path")
		}
		absPath, err := filepath.Abs(filename)
		if err != nil {
			panic(err)
		}
		path := filepath.Clean(absPath)
		parts := strings.Split(path, string(filepath.Separator))
		projectPath := string(filepath.Separator) + filepath.Join(parts[1:len(parts)-4]...)
		configPath = filepath.Join(projectPath, "internal/config")
	}
	return configPath
}

func WriteToFile(filename string) error {
	progressFile := filepath.Join(GetConfigPath(), "progress.txt")
	file, err := os.OpenFile(progressFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(filename + "\n"); err != nil {
		fmt.Println("追加内容失败:", err)
		return err
	} else {
		return nil
	}
}

func ReadJSONFile(filename string) (map[string]interface{}, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("读取 JSON 文件失败: %w", err)
	}

	var mapping map[string]interface{}
	if err := json.Unmarshal(data, &mapping); err != nil {
		return nil, fmt.Errorf("解析 JSON 数据失败: %w", err)
	}

	return mapping, nil
}
