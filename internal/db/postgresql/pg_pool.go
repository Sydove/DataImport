package postgresql

import (
	redisPool "DataImport/internal/db/redis"
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
)

var Pool *pgxpool.Pool

func InitDB() error {
	ctx := context.Background()
	writeDSN := viper.GetString("postgresql.writeDSN")
	config, err := pgxpool.ParseConfig(writeDSN)
	if err != nil {
		log.Fatalf("parse config failed: %v", err)
		return err
	}

	config.MaxConns = 8
	config.MinConns = 4
	config.MaxConnIdleTime = 10 * time.Minute
	config.MaxConnLifetime = 1 * time.Hour
	config.HealthCheckPeriod = 30 * time.Second

	Pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
		return err
	}

	log.Println("postgresql 连接池初始化成功")
	return nil
}

type Record struct {
	Content   string    `json:"content"`
	Text      string    `json:"text"`
	OriginId  string    `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	AccountId int       `json:"account_id"`
}

// BatchInsert
//
//	@Description: 批量插入数据 3.376283s
//	@param rows
//	@return error
func BatchInsert(rows []Record) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if len(rows) == 0 {
		return nil
	}

	builder := strings.Builder{}
	builder.WriteString(`INSERT INTO article (created_at, updated_at, title, content, account_id, origin_id) VALUES `)

	for i, item := range rows {
		if i > 0 {
			builder.WriteString(",")
		}
		// 不使用占位符效率更高,但需要手动转义单引号
		builder.WriteString(fmt.Sprintf("('%s','%s','%s','%s',%d,'%s')",
			item.CreatedAt.Format("2006-01-02 15:04:05"),
			item.UpdatedAt.Format("2006-01-02 15:04:05"),
			strings.ReplaceAll(item.Title, "'", "''"),
			strings.ReplaceAll(item.Content, "'", "''"),
			item.AccountId,
			item.OriginId,
		))
	}

	query := builder.String()
	_, err := Pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("batch insert error: %w", err)
	}

	fmt.Printf("批量插入成功: %d 条记录\n", len(rows))
	elapsed := time.Since(start) // 计算耗时
	fmt.Printf("批量插入的程序总共耗时: %v\n", elapsed)
	return nil
}

// BatchInsertWithParams
//
//	@Description: 使用占位符批量插入数据 3.843323125s
//	@param rows
//	@return error
func BatchInsertWithParams(rows []Record) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if len(rows) == 0 {
		return nil
	}

	// 构建批量插入 SQL
	baseQuery := `INSERT INTO article (created_at, updated_at, title, content, account_id, origin_id) VALUES `
	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows)*6)

	for i, item := range rows {
		// 每 6 个参数一组
		start := i*6 + 1
		valueStrings = append(valueStrings,
			fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
				start, start+1, start+2, start+3, start+4, start+5,
			),
		)
		valueArgs = append(valueArgs,
			item.CreatedAt, item.UpdatedAt, item.Title, item.Content, item.AccountId, item.OriginId,
		)
	}

	query := baseQuery + strings.Join(valueStrings, ",")
	_, err := Pool.Exec(ctx, query, valueArgs...)
	if err != nil {
		return fmt.Errorf("batch insert error: %w", err)
	}

	fmt.Printf("批量插入成功: %d 条记录\n", len(rows))
	elapsed := time.Since(start) // 计算耗时
	fmt.Printf("批量插入带有占位符的程序总共耗时: %v\n", elapsed)
	return nil
}

// BatchCopyInsert
//
//	@Description: 使用 CopyFrom 批量导入数据 2.161921125s
//	@param rows
//	@return error
func BatchCopyInsert(rows []Record) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if len(rows) == 0 {
		return nil
	}

	// 从连接池获取连接
	conn, err := Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("获取连接失败: %w", err)
	}
	defer conn.Release()

	// 将 Record 转为 [][]interface{}
	data := make([][]interface{}, 0, len(rows))
	for _, item := range rows {
		data = append(data, []interface{}{
			item.CreatedAt,
			item.UpdatedAt,
			item.Title,
			item.Content,
			item.AccountId,
			item.OriginId,
		})
	}

	// 使用 CopyFrom 执行批量导入
	copyCount, err := conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{"article"}, // 表名
		[]string{"created_at", "updated_at", "title", "content", "account_id", "origin_id"}, // 列名
		pgx.CopyFromRows(data), // 数据
	)
	if err != nil {
		return fmt.Errorf("CopyFrom 执行失败: %w", err)
	}

	fmt.Printf("批量导入成功，共导入 %d 条记录\n", copyCount)
	return nil
}

// StreamReadArticle cursor流式读取 2m8.380 执行效率更低相比ID逻辑分页
//
//	@Description:
//	@return []Record
func StreamReadArticle() []Record {
	ctx, cancel := context.WithTimeout(context.Background(), 120000*time.Second)
	defer cancel()
	conn, err := Pool.Acquire(ctx)
	if err != nil {
		return []Record{}
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return []Record{}
	}
	defer tx.Rollback(ctx)

	cursorName := "data_cursor"
	query := fmt.Sprintf(`
		DECLARE %s CURSOR FOR
		SELECT id, title FROM article ORDER BY id
	`, cursorName)
	_, err = tx.Exec(ctx, query)
	if err != nil {
		log.Fatalf("failed to declare cursor: %v", err)
	}
	batchSize := 10000
	total := 0

	for {
		//批量拉取数据
		rows, err := tx.Query(ctx, fmt.Sprintf("FETCH FORWARD %d FROM %s", batchSize, cursorName))
		if err != nil {
			log.Fatalf("fetch failed: %v", err)
		}

		count := 0
		for rows.Next() {
			var id int
			var content string
			if err := rows.Scan(&id, &content); err != nil {
				log.Fatalf("scan failed: %v", err)
			}
			// 这里可以进行处理，例如去重、写Redis、导出文件等
			count++
		}
		rows.Close()

		if count == 0 {
			break // 已经读完
		}

		total += count
		fmt.Printf("已读取 %d 行\n", total)
	}

	//关闭游标并提交事务
	_, _ = tx.Exec(ctx, fmt.Sprintf("CLOSE %s", cursorName))
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("commit failed: %v", err)
	}

	fmt.Println("流式读取完成 ✅")
	return []Record{}
}

// ReadArticleById  根据id翻页读取数据 1m44.41987
//
//	@Description:
//	@return []Record
func ReadArticleById() []Record {
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	pool := redisPool.InitPool()
	defer pool.Close()
	redisConn := pool.Get()
	defer redisConn.Close()

	// 清空集合
	redisConn.Do("DEL", "articles")

	total, pageSize := 12002284, 10000
	totalPage := (total + pageSize - 1) / pageSize
	distinctMap := make(map[string]int64)

	for page := 0; page < totalPage; page++ {
		startId := page*pageSize + 1
		query, err := Pool.Query(ctx, "SELECT origin_id, title FROM article WHERE id >= $1 LIMIT $2", startId, pageSize)
		if err != nil {
			fmt.Printf("query failed: %v\n", err)
			return []Record{}
		}

		count := 0
		for query.Next() {
			var originId, content string
			if err := query.Scan(&originId, &content); err != nil {
				fmt.Printf("scan failed: %v\n", err)
				return []Record{}
			}
			// 去重处理
			distinctMap[originId] = distinctMap[originId] + 1
			count++
		}
		query.Close()
		fmt.Printf("处理了%d条数据\n", count)

		if count == 0 {
			break
		}
	}
	for k, v := range distinctMap {
		if v >= 1 {
			fmt.Printf("origin_id: %s, count: %d\n", k, v)
		}
	}
	return []Record{}
}
