package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func GetProjectPath() string {
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
	return projectPath
}

func WriteToFile(filename string) error {
	progressFile := filepath.Join(GetProjectPath(), "progress.txt")
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
