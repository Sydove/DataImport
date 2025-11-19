package main

import (
	"DataImport/internal/db/postgresql"
	_ "DataImport/internal/pkg/config"
	"fmt"
	"time"
)

func main() {
	start := time.Now()
	err := postgresql.InitDB()
	if err != nil {
		fmt.Printf("init postgresql failed: %v", err)
		panic(err)
	}
	//postgresql.StreamReadArticle()
	postgresql.ReadArticleById()
	elapsed := time.Since(start) // 计算耗时
	fmt.Printf("程序总共耗时: %v\n", elapsed)
}
