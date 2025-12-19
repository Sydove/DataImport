package es

import (
	_ "DataImport/internal/pkg/config"
	"context"
	"fmt"
	"testing"
)

func TestES(t *testing.T) {
	// 初始化客户端
	esClient := NewESClient()
	fmt.Println(esClient)
	clusterInfo, err := esClient.GetClusterInfo(context.Background())
	if err != nil {
		t.Errorf("GetClusterInfo failed: %v", err)
	}
	// 打印集群信息
	fmt.Printf("%#v\n", clusterInfo)

	//if err := esClient.DeleteIndex(context.Background(), "article"); err != nil {
	//	t.Errorf("DeleteIndex failed: %v", err)
	//}

	//// 创建索引
	//projectPath := utils.GetProjectPath()
	//mappingPath := filepath.Join(projectPath, "internal/config/article.json")
	//// 读取映射文件
	//mapping, err := utils.ReadJSONFile(mappingPath)
	//if err != nil {
	//	t.Errorf("ReadJSONFile failed: %v", err)
	//}
	//fmt.Printf("%#v\n", mapping)
	//if err := esClient.CreateIndexWithMapping(context.Background(), "article", mapping); err != nil {
	//	t.Errorf("CreateIndexWithMapping failed: %v", err)
	//}

	//insertErr := esClient.InsertSingleDocument(context.Background(), "article", "1", map[string]interface{}{
	//	"title":      "React 简介和“环境搭建” | React 基础理论实操(modify)",
	//	"content":    ".1 React 初认识\nReact16 和之后的版本，我们称之为“React Fiber”，其性能和便捷度上都得到了大幅提升。在 React 的使用中，我们会看到全球范围内的开发者解决同一个问题时的不同方案。\n所以，学习 React 可以让我们快速地了解前端技术在全球范围内的发展情况。\nReact 有很多相关的技术点：\n\nReact JS：可以用 React 的语法来编写一些网页的交互效果；\nReact Native：可以让我们借用 React 的语法来编写原生的 APP 应用；\nReact VR / React 360：可以在 React 的语法基础上，去开发一些 VR / 全景应用。\n\n本专栏，我们主要学习 React JS 部分。\nReact JS 是 Facebook 在 2013 年 5 月开源推出的一款前端框架，其带来了一种全新的“函数式”编程风格，是全球范围内使用人数最多的一款前端框架。它也拥有全球范围内最健全的文档和完善的社区。\n\n作者：itsOli\n链接：https://juejin.cn/post/7238631475553583163\n来源：稀土掘金\n著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。",
	//	"origin_id":  "7238631475553583163",
	//	"created_at": "2025-01-01 00:00:00",
	//	"id":         1,
	//	"account_id": 1,
	//})
	//if insertErr != nil {
	//	t.Errorf("insert failed: %v", insertErr)
	//}

	result, searchErr := esClient.SearchAll(context.Background(), "article", 10)
	if searchErr != nil {
		t.Errorf("SearchAll failed: %v", searchErr)
	}
	fmt.Printf("%#v\n", result)
	total, err := esClient.Count(context.Background(), "article")
	if err != nil {
		t.Errorf("Count failed: %v", err)
	}
	fmt.Printf("%#v\n", total)

}
