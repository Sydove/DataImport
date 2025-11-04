package utils

import (
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
