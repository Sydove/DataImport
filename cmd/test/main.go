package main

import "fmt"

func main() {
	distinctMap := make(map[int64]int64)
	for i := 0; i < 10; i++ {
		distinctMap[int64(i)] = distinctMap[int64(i)] + 1
	}
	fmt.Println(distinctMap)
}
