package utils

import (
	"fmt"
	"time"
)

func Retry(attempts int, sleep time.Duration, fn func() ([]string, error)) ([]string, error) {
	var err error
	var errIds []string
	for i := 0; i < attempts; i++ {
		errIds, err = fn()
		if err == nil {
			return errIds, nil
		}
		time.Sleep(sleep)
	}
	return errIds, fmt.Errorf("重试 %d 次后仍然失败: %w", attempts, err)
}
