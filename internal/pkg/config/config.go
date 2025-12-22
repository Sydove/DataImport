package config

import (
	"DataImport/internal/pkg/utils"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

func init() {
	mode := os.Getenv("PROJECT_MODE")
	projectPath := utils.GetConfigPath()
	switch mode {
	case "prod":
		viper.SetConfigFile(filepath.Join(projectPath, "config.yaml"))
	default:
		viper.SetConfigFile(filepath.Join(projectPath, "config.yaml"))
	}
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	viper.WatchConfig()
	fmt.Println("配置文件读取成功!")
}
