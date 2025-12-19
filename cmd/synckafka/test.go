package main

import (
	"fmt"
)

func main() {
	s := []string{"a", "b", "c", "d", "e", "f"}
	b := s[2:]
	fmt.Printf("base数组的地址: %v\n", &s[2])
	fmt.Println(b, len(b), cap(b), &b[0])

	// 修改切片,查看数组情况
	b[0] = "z"
	fmt.Println(s)
	fmt.Printf("修改后base数组的地址: %v\n", &s[2])
	fmt.Println(b, len(b), cap(b), &b[0])

}
