package es

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/spf13/viper"
)

type ESClient struct {
	client     *elasticsearch.Client
	MaxRetries int
}

func NewESClient() *ESClient {
	addr := viper.GetString("elasticsearch.addr")
	username := viper.GetString("elasticsearch.user")
	password := viper.GetString("elasticsearch.passwd")
	cfg := elasticsearch.Config{
		Addresses:  []string{addr},
		Username:   username,
		Password:   password,
		MaxRetries: 3,
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	return &ESClient{client, 3}
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
	defer func() { _ = res.Body.Close() }()
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

type BulkResponse struct {
	Took   int  `json:"took"`
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			Index   string `json:"_index"`
			ID      string `json:"_id"`
			Version int    `json:"_version"`
			Result  string `json:"result"`
			Shards  struct {
				Total      int `json:"total"`
				Successful int `json:"successful"`
				Failed     int `json:"failed"`
			} `json:"_shards"`
			SeqNo       int `json:"_seq_no"`
			PrimaryTerm int `json:"_primary_term"`
			Status      int `json:"status"`
			Error       struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error,omitempty"`
		} `json:"index"`
	} `json:"items"`
}

type BulkStats struct {
	Total     int `json:"total"`
	Created   int `json:"created"`
	Updated   int `json:"updated"`
	Failed    int `json:"failed"`
	Noops     int `json:"noops"`     // 无变化更新
	Deleted   int `json:"deleted"`   // 删除操作
	Conflicts int `json:"conflicts"` // 版本冲突
}

func (resp *BulkResponse) GenerateStats() (BulkStats, []string) {
	var stats BulkStats
	failedItems := make([]string, 0)
	stats.Total = len(resp.Items)
	for _, item := range resp.Items {
		if item.Index.Error.Type == "version_conflict_engine_exception" {
			stats.Conflicts++
		}
		switch {
		case item.Index.Status >= 400:
			failedItems = append(failedItems, item.Index.ID)
			stats.Failed++
		case item.Index.Result == "created":
			stats.Created++
		case item.Index.Result == "updated":
			stats.Updated++
		case item.Index.Result == "noop":
			stats.Noops++
		case item.Index.Result == "deleted":
			stats.Deleted++
		}
	}
	return stats, failedItems
}

// BulkInsertDocuments
//
//	@Description: 批量插入elasticsearch
//	@receiver es
//	@return unc
func (es *ESClient) BulkInsertDocuments(consumerID int, ctx context.Context, indexName string, documents []map[string]interface{}) ([]string, error) {
	if len(documents) == 0 {
		return nil, nil
	}

	// 预分配足够的空间给strings.Builder
	estSize := len(documents) * 1000 // 根据实际情况调整预估大小
	bulkBody, startID, endID, err := buildBulkRequestBody(indexName, documents, estSize)
	if err != nil {
		return nil, err
	}

	// 执行批量请求
	res, err := esapi.BulkRequest{
		Body: strings.NewReader(bulkBody),
	}.Do(ctx, es.client)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("bulk 请求失败: %s, 响应: %s", res.Status(), string(body))
	}

	// 解析响应，提取失败ID
	var resp BulkResponse
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	// 收集失败ID
	stats, failedIDs := resp.GenerateStats()
	fmt.Printf("consumerID-%d完成任务:\n插入范围%s-%s", consumerID, startID, endID)
	fmt.Printf("执行任务%d条\n创建记录%d条\n更新%d条\n失败%d条\n", stats.Total, stats.Created, stats.Updated, len(failedIDs))
	fmt.Println("****************")

	if resp.Errors || len(failedIDs) > 0 {
		return failedIDs, fmt.Errorf("bulk 写入存在失败文档: total=%d failed=%d conflicts=%d", stats.Total, stats.Failed, stats.Conflicts)
	}
	return failedIDs, nil
}

func buildBulkRequestBody(indexName string, documents []map[string]interface{}, estSize int) (string, string, string, error) {
	var bulkBody strings.Builder
	if estSize > 0 {
		bulkBody.Grow(estSize)
	}

	startID := "unknown"
	if id, ok := documentIDString(documents[0]["id"]); ok {
		startID = id
	}

	endID := "unknown"
	if id, ok := documentIDString(documents[len(documents)-1]["id"]); ok {
		endID = id
	}

	// 构建批量请求体
	for _, doc := range documents {
		// 准备元数据
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
			},
		}

		// 处理文档ID（如果不提供ID，Elasticsearch会自动生成）
		if id, ok := documentIDString(doc["id"]); ok {
			meta["index"].(map[string]interface{})["_id"] = id
		}

		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return "", "", "", fmt.Errorf("序列化 bulk 元数据失败: %w", err)
		}
		docJSON, err := json.Marshal(doc)
		if err != nil {
			return "", "", "", fmt.Errorf("序列化 bulk 文档失败: %w", err)
		}

		bulkBody.Write(metaJSON)
		bulkBody.WriteByte('\n')
		bulkBody.Write(docJSON)
		bulkBody.WriteByte('\n')
	}

	return bulkBody.String(), startID, endID, nil
}

func documentIDString(id interface{}) (string, bool) {
	switch value := id.(type) {
	case nil:
		return "", false
	case string:
		return value, value != ""
	case json.Number:
		return value.String(), value.String() != ""
	case int:
		return strconv.Itoa(value), value != 0
	case int32:
		return strconv.FormatInt(int64(value), 10), value != 0
	case int64:
		return strconv.FormatInt(value, 10), value != 0
	case uint:
		return strconv.FormatUint(uint64(value), 10), value != 0
	case uint32:
		return strconv.FormatUint(uint64(value), 10), value != 0
	case uint64:
		return strconv.FormatUint(value, 10), value != 0
	case float32:
		return formatFloatID(float64(value))
	case float64:
		return formatFloatID(value)
	default:
		return "", false
	}
}

func formatFloatID(value float64) (string, bool) {
	if value == 0 {
		return "", false
	}
	if math.Trunc(value) == value {
		return strconv.FormatInt(int64(value), 10), true
	}
	return strconv.FormatFloat(value, 'f', -1, 64), true
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
