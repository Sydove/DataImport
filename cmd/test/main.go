package main

import "fmt"

func main() {
	endData := 100000
	for i := 101; i <= endData; i += 100 {
		result := int32(i % 6)
		fmt.Println(result)
	}
}
