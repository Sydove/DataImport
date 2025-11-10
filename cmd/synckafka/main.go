package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				fmt.Println("first goroutine exited")
				return
			default:
				fmt.Println("first goroutine running")
			}
		}

	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				fmt.Println("second goroutine exited")
				return
			default:
				fmt.Println("second goroutine running")
			}
		}

	}()
	time.Sleep(2 * time.Second)
	cancel()
	wg.Wait()
}
