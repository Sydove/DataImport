package postgresql

import (
	"context"
	"fmt"
	"log"
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

func BatchInsert(rows []Record) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	batch := &pgx.Batch{}
	query := `INSERT INTO article (created_at, updated_at,title,content,account_id,origin_id) VALUES ($1, $2, $3, $4, $5, $6)`
	for _, item := range rows {
		batch.Queue(query, item.CreatedAt, item.UpdatedAt, item.Title, item.Content, item.AccountId, item.OriginId)
	}
	results := Pool.SendBatch(ctx, batch)
	defer results.Close()
	for range rows {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("batch exec error: %w", err)
		}
	}
	fmt.Println("batch insert success")
	return nil
}
