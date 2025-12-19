package main

/*
单个goroutine从postgresql读取数据,推送个体到jobs channel
多个goroutine从jobs channel消费数据,批量写入kafka
多个producer从kafka消费数据,批量写入es
postgresql → [Reader goroutine] → jobs channel → [N Sender goroutines] → Kafka → [M Consumers] → ES (Bulk)
*/

import (
	"DataImport/internal/db/postgresql"
	_ "DataImport/internal/pkg/config"
	"DataImport/internal/pkg/es"
	mKakfa "DataImport/internal/pkg/kafka"
	"DataImport/internal/pkg/utils"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	kafkaGo "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5"
)

const (
	TopicName    = "full_load"
	DLQTopicName = "full_load_dlq"
	IndexName    = "article"
)

type DataReocrd struct {
	Content   string    `json:"content"`
	OriginId  string    `json:"origin_id"`
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	AccountId int       `json:"account_id"`
}

// readFromPostgres
//
//	@Description: 从postgresql读取数据
//	@param ctx
//	@param pageSize limit条数
//	@param startId 开始id
//	@return []postgresql.Record
func readFromPostgres(ctx context.Context, pageSize, startId int) []DataReocrd {
	rows, err := postgresql.Pool.Query(ctx, "SELECT content, origin_id, id, title, created_at, account_id FROM article WHERE id >= $1 order by id LIMIT $2", startId, pageSize)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	results, err := pgx.CollectRows(rows, pgx.RowToStructByName[DataReocrd])
	if err != nil {
		panic(err)
	}
	// 限制时间精度到毫秒,es最多支持到毫秒级别(3位小数)
	for i := range results {
		results[i].CreatedAt = results[i].CreatedAt.Truncate(time.Millisecond)
	}
	return results
}

// pushToChannel
//
//	@Description: 读取数据推送到channel
func pushToChannel(job chan []DataReocrd, producer *mKakfa.Producer) {
	defer close(job)

	startId := 1

	for {
		// 读取 postgres 超时时间
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		records := readFromPostgres(ctx, 100, startId)
		cancel()

		// postgres 没数据 → 退出
		if len(records) == 0 {
			fmt.Println("Postgres: 没有更多数据，pushToChannel 退出")
			return
		}
		select {
		case <-producer.StopCtx.Done():
			fmt.Println("收到关闭信号，pushToChannel 退出")
			return
		case job <- records:
			// 数据成功推入
			startId = records[len(records)-1].ID
		}
	}
}

// producer
//
//	@Description: 从channel推送数据到kafka
func producer(client *mKakfa.Client, group *sync.WaitGroup, stopCtx context.Context) {
	job := make(chan []DataReocrd, 3)
	err := client.NewClientWithConfig(mKakfa.ProducerConfig{
		CompressionType: "snappy",
		BatchSize:       51200,
		LingerMs:        10,
		Acks:            "all",
		MaxPending:      10000, // 背压阈值
	}, stopCtx)
	if err != nil {
		panic(err)
	}
	mProducer := client.GetProducer()
	mProducer.Producer.GetMetadata(nil, true, 5000)
	go pushToChannel(job, mProducer)
	var topicName = TopicName
	for i := 0; i < 3; i++ {
		group.Add(1)
		go func(partitionId int) {
			defer group.Done()
			for record := range job {
				//  批量kafka
				for _, item := range record {
					jsonData, err := json.Marshal(item)
					if err != nil {
						fmt.Println("序列化失败:", err)
						return
					}
					sendMsg := &kafkaGo.Message{
						TopicPartition: kafkaGo.TopicPartition{
							Topic:     &topicName,
							Partition: int32(partitionId),
						},
						Value: jsonData,
					}
					if err := mProducer.Produce(sendMsg); err != nil {
						fmt.Printf("Produce message failed: %v\n", err)
					}
				}

			}
		}(i)
	}
}

func pushToElasticSearch(msg *kafkaGo.Message, esClient *es.ESClient) error {
	// 解析消息
	var record map[string]interface{}
	if err := json.Unmarshal(msg.Value, &record); err != nil {
		fmt.Println("反序列化失败:", err)
		return err
	}
	//fmt.Println(record)
	docId := record["id"].(float64)
	docIdStr := strconv.FormatInt(int64(docId), 10)
	insertErr := esClient.InsertSingleDocument(context.Background(), IndexName, docIdStr, record)
	if insertErr != nil {
		fmt.Printf("插入失败: %v\n", docId)
	}
	return nil
}

// consumer
//
//	@Description: 从kafka消费数据,批量写入es
func consumer(client *mKakfa.Client, group *sync.WaitGroup, consumerID int, total *int64, esClient *es.ESClient, stopCtx context.Context) error {
	defer group.Done()
	consumer, err := client.CreateConsumer(mKakfa.ConsumerConfig{
		GroupID:         "number_one",
		Topics:          []string{TopicName},
		AutoOffsetReset: "earliest",
		CommitBatchSize: 11,
	}, consumerID, stopCtx)
	if err != nil {
		return err
	}

	// 开始消费
	consumer.StartConsuming(consumerID, pushToElasticSearch, total, esClient)
	return nil

}

func createEsIndex(esClient *es.ESClient) error {
	projectPath := utils.GetProjectPath()
	mappingPath := filepath.Join(projectPath, "internal/config/article.json")
	// 读取映射文件
	mapping, err := utils.ReadJSONFile(mappingPath)
	if err != nil {
		return err
	}
	fmt.Printf("%#v\n", mapping)
	if err := esClient.CreateIndexWithMapping(context.Background(), "article", mapping); err != nil {
		return err
	}
	return nil
}

func main() {
	waitGroup := sync.WaitGroup{}

	cancelCtx, cancel := context.WithCancel(context.Background())
	// 初始化pgsql
	if err := postgresql.InitDB(); err != nil {
		panic(err)
	}
	// 创建kafka topic
	kafkaClient, err := mKakfa.NewClient()
	if err != nil {
		panic(err)
	}

	// 创建ES
	esClient := es.NewESClient()

	// 初始化索引
	if err := esClient.DeleteIndex(context.Background(), IndexName); err != nil {
		panic(err)
	}
	if err := createEsIndex(esClient); err != nil {
		panic(err)
	}

	//topicConfig := mKakfa.TopicConfig{
	//	Name:              TopicName,
	//	NumPartitions:     3,
	//	ReplicationFactor: 3,
	//}

	// 删除topic
	//if err := kafkaClient.DeleteTopic(TopicName); err != nil {
	//	panic(err)
	//}
	//
	//if err := kafkaClient.CreateTopic(topicConfig); err != nil {
	//	panic(err)
	//}
	//if err := kafkaClient.WaitTopicReady(TopicName, 300*time.Second); err != nil {
	//	panic(err)
	//}

	go producer(kafkaClient, &waitGroup, cancelCtx)

	var total int64 = 0
	for i := 0; i < 3; i++ {
		waitGroup.Add(1)
		go func(consumerID int) {
			if err := consumer(kafkaClient, &waitGroup, consumerID, &total, esClient, cancelCtx); err != nil {
				fmt.Printf("Consumer %d 错误: %v\n", consumerID, err)
			}
		}(i + 1)
	}

	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
		<-signalChan
		fmt.Println("收到退出信号，正在关闭...")
		fmt.Printf("一共消费%d\n", total)
		cancel()
	}()

	waitGroup.Wait()
	kafkaClient.Close()
	fmt.Printf("一共消费%d\n", total)
	fmt.Println("所有 goroutine 已运行结束，程序结束")
}
