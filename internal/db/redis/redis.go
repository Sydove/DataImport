package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func InitRedisClient() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "192.168.31.195:30777",
		Password: "lql@123",
		DB:       0, // 默认数据库
	})

	err := rdb.Set(ctx, "key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println("key:", val)
}
