package redis

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

func InitPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		MaxActive:   50,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "192.168.31.195:30777")
			if err != nil {
				return nil, err
			}

			if _, err := c.Do("AUTH", "lql@123"); err != nil {
				c.Close()
				return nil, err
			}

			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
