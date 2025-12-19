package es

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/spf13/viper"
)

type ESClient struct {
	client *elasticsearch.Client
}

func NewESClient() *ESClient {
	addr := viper.GetString("elasticsearch.addr")
	username := viper.GetString("elasticsearch.user")
	password := viper.GetString("elasticsearch.passwd")
	cfg := elasticsearch.Config{
		Addresses: []string{addr},
		Username:  username,
		Password:  password,
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	return &ESClient{client}
}

// GetClusterInfo
//
//	@Description: 获取 Elasticsearch 集群信息
//	@receiver es
//	@param ctx
//	@return map[string]interface{}
//	@return error
func (es *ESClient) GetClusterInfo(ctx context.Context) (map[string]interface{}, error) {
	res, err := es.client.Info(es.client.Info.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, errors.New(res.String())
	}

	var info map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		return nil, err
	}
	return info, nil
}

// CreateIndex
//
//	@Description: 创建索引
//	@receiver es
//	@param ctx
//	@param indexName
//	@return error
func (es *ESClient) CreateIndex(ctx context.Context, indexName string) error {
	res, err := es.client.Indices.Create(indexName, es.client.Indices.Create.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("创建索引失败: %s, 响应: %s", res.Status(), string(body))
	}
	return nil
}

// CreateIndexWithMapping
//
//	@Description: 创建带mapping的索引
//	@receiver es
//	@param ctx
//	@param indexName
//	@param mapping
//	@return error
func (es *ESClient) CreateIndexWithMapping(ctx context.Context, indexName string, mapping map[string]interface{}) error {
	bodyJSON, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("序列化 mapping 失败: %w", err)
	}

	res, err := es.client.Indices.Create(
		indexName,
		es.client.Indices.Create.WithContext(ctx),
		es.client.Indices.Create.WithBody(bytes.NewReader(bodyJSON)),
	)
	if err != nil {
		return fmt.Errorf("创建索引失败: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("创建索引失败: %s, 响应: %s", res.Status(), string(body))
	}

	return nil
}

// IndexExists
//
//	@Description: 判断索引是否存在
//	@receiver es
//	@param ctx
//	@param indexName
//	@return bool
//	@return error
func (es *ESClient) IndexExists(ctx context.Context, indexName string) (bool, error) {
	res, err := es.client.Indices.Exists(
		[]string{indexName},
		es.client.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, fmt.Errorf("检查索引存在失败: %w", err)
	}
	defer res.Body.Close()

	return res.StatusCode == 200, nil
}

// InsertSingleDocument
//
//	@Description: 插入单个文档
//	@receiver es
//	@param ctx
//	@param indexName
//	@param docID
//	@param document
//	@return error
func (es *ESClient) InsertSingleDocument(ctx context.Context, indexName, docID string, document interface{}) error {
	docJSON, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("序列化文档失败: %w", err)
	}
	// put方法,相同的_id会覆盖之前的内容
	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: docID,
		Body:       bytes.NewReader(docJSON),
		Refresh:    "true", // 立即刷新,测试时使用,生产环境建议去掉

	}

	res, err := req.Do(ctx, es.client)
	if err != nil {
		return fmt.Errorf("插入文档失败: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("插入文档失败: %s, 响应: %s", res.Status(), string(body))
	}

	return nil
}

// BulkInsertDocuments
//
//	@Description: 批量插入文档
//	@receiver es
//	@param ctx
//	@param indexName
//	@param documents
//	@return error
func (es *ESClient) BulkInsertDocuments(ctx context.Context, indexName string, documents []map[string]interface{}) error {
	// 创建 BulkIndexer
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         indexName,
		Client:        es.client,
		NumWorkers:    4,                // 并发 worker 数量
		FlushBytes:    5e+6,             // 5MB 刷新一次
		FlushInterval: 30 * time.Second, // 30 秒刷新一次
	})
	if err != nil {
		return fmt.Errorf("创建 BulkIndexer 失败: %w", err)
	}

	// 添加文档到批量操作
	for i, doc := range documents {
		docJSON, err := json.Marshal(doc)
		if err != nil {
			log.Printf("序列化文档 %d 失败: %v", i, err)
			continue
		}

		// 如果文档中有 id 字段,使用它作为文档 ID
		docID := ""
		if id, ok := doc["id"]; ok {
			docID = fmt.Sprintf("%v", id)
		}

		err = bi.Add(
			ctx,
			esutil.BulkIndexerItem{
				Action:     "index",
				DocumentID: docID,
				Body:       bytes.NewReader(docJSON),
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					// 成功回调(可选)
				},
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Printf("批量插入失败: %v", err)
					} else {
						log.Printf("批量插入失败: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			log.Printf("添加文档到 BulkIndexer 失败: %v", err)
		}
	}

	// 关闭 BulkIndexer,等待所有操作完成
	if err := bi.Close(ctx); err != nil {
		return fmt.Errorf("关闭 BulkIndexer 失败: %w", err)
	}

	stats := bi.Stats()
	log.Printf("批量插入完成: 成功 %d, 失败 %d", stats.NumFlushed, stats.NumFailed)

	return nil
}

// Count
//
//	@Description: 计数
//	@receiver es
//	@param ctx
//	@param indexName
//	@return int64
//	@return error
func (es *ESClient) Count(ctx context.Context, indexName string) (int64, error) {
	res, err := es.client.Count(
		es.client.Count.WithContext(ctx),
		es.client.Count.WithIndex(indexName),
	)
	if err != nil {
		return 0, fmt.Errorf("查询总数失败: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("查询总数失败: %s", res.Status())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("解析响应失败: %w", err)
	}

	count := int64(result["count"].(float64))
	return count, nil
}

// SearchAll
//
//	@Description: 查询所有
//	@receiver es
//	@param ctx
//	@param indexName
//	@param size
//	@return []map[string]interface{}
//	@return error
func (es *ESClient) SearchAll(ctx context.Context, indexName string, size int) ([]map[string]interface{}, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"size": size,
	}

	return es.search(ctx, indexName, query)
}

// SearchByTerm
//
//	@Description: 条件查询
//	@receiver es
//	@param ctx
//	@param indexName
//	@param field
//	@param value
//	@param size
//	@return []map[string]interface{}
//	@return error
func (es *ESClient) SearchByTerm(ctx context.Context, indexName, field string, value interface{}, size int) ([]map[string]interface{}, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				field: value,
			},
		},
		"size": size,
	}

	return es.search(ctx, indexName, query)
}

// SearchByMatch
//
//	@Description: 全文搜索
//	@receiver es
//	@param ctx
//	@param indexName
//	@param field
//	@param text
//	@param size
//	@return []map[string]interface{}
//	@return error
func (es *ESClient) SearchByMatch(ctx context.Context, indexName, field, text string, size int) ([]map[string]interface{}, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				field: text,
			},
		},
		"size": size,
	}

	return es.search(ctx, indexName, query)
}

// SearchByRange
//
//	@Description: 范围查询
//	@receiver es
//	@param ctx
//	@param indexName
//	@param field
//	@param gte
//	@param lte
//	@param size
//	@return []map[string]interface{}
//	@return error
func (es *ESClient) SearchByRange(ctx context.Context, indexName, field string, gte, lte interface{}, size int) ([]map[string]interface{}, error) {
	rangeQuery := map[string]interface{}{}
	if gte != nil {
		rangeQuery["gte"] = gte
	}
	if lte != nil {
		rangeQuery["lte"] = lte
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				field: rangeQuery,
			},
		},
		"size": size,
	}

	return es.search(ctx, indexName, query)
}

// SearchWithPagination
//
//	@Description: 分页查询
//	@receiver es
//	@param ctx
//	@param indexName
//	@param from
//	@param size
//	@return []map[string]interface{}
//	@return error
func (es *ESClient) SearchWithPagination(ctx context.Context, indexName string, from, size int) ([]map[string]interface{}, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"from": from,
		"size": size,
	}

	return es.search(ctx, indexName, query)
}

// search
//
//	@Description: 通用搜索
//	@receiver es
//	@param ctx
//	@param indexName
//	@param query
//	@return []map[string]interface{}
//	@return error
func (es *ESClient) search(ctx context.Context, indexName string, query map[string]interface{}) ([]map[string]interface{}, error) {
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("序列化查询失败: %w", err)
	}

	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(indexName),
		es.client.Search.WithBody(bytes.NewReader(queryJSON)),
		es.client.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, fmt.Errorf("搜索失败: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("搜索失败: %s, 响应: %s", res.Status(), string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	// 提取文档
	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
	documents := make([]map[string]interface{}, 0, len(hits))

	for _, hit := range hits {
		hitMap := hit.(map[string]interface{})
		source := hitMap["_source"].(map[string]interface{})
		// 添加 _id 到结果中
		source["_id"] = hitMap["_id"]
		documents = append(documents, source)
	}

	return documents, nil
}

// DeleteDocument
//
//	@Description: 删除文档
//	@receiver es
//	@param ctx
//	@param indexName
//	@param docID
//	@return error
func (es *ESClient) DeleteDocument(ctx context.Context, indexName, docID string) error {
	req := esapi.DeleteRequest{
		Index:      indexName,
		DocumentID: docID,
		Refresh:    "true",
	}

	res, err := req.Do(ctx, es.client)
	if err != nil {
		return fmt.Errorf("删除文档失败: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("删除文档失败: %s", res.Status())
	}

	return nil
}

// UpdateDocument
//
//	@Description: 更新文档
//	@receiver es
//	@param ctx
//	@param indexName
//	@param docID
//	@param doc
//	@return error
func (es *ESClient) UpdateDocument(ctx context.Context, indexName, docID string, doc map[string]interface{}) error {
	updateBody := map[string]interface{}{
		"doc": doc,
	}

	docJSON, err := json.Marshal(updateBody)
	if err != nil {
		return fmt.Errorf("序列化文档失败: %w", err)
	}

	req := esapi.UpdateRequest{
		Index:      indexName,
		DocumentID: docID,
		Body:       bytes.NewReader(docJSON),
		Refresh:    "true",
	}

	res, err := req.Do(ctx, es.client)
	if err != nil {
		return fmt.Errorf("更新文档失败: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("更新文档失败: %s, 响应: %s", res.Status(), string(body))
	}

	return nil
}

// DeleteIndex
//
//	@Description: 删除索引
//	@receiver es
//	@param ctx
//	@param indexName
//	@return error
func (es *ESClient) DeleteIndex(ctx context.Context, indexName string) error {
	res, err := es.client.Indices.Delete(
		[]string{indexName},
		es.client.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("删除索引失败: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("删除索引失败: %s", res.Status())
	}

	return nil
}
