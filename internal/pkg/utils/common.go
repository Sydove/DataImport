package utils

import (
	"fmt"
	"time"
)

func Retry(attempts int, sleep time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		time.Sleep(sleep)
	}
	return fmt.Errorf("重试 %d 次后仍然失败: %w", attempts, err)
}
