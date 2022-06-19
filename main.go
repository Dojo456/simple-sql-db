package main

import (
	"fmt"
	"github.com/Dojo456/simple-inmem-db/frontend"
)

func main() {
	var input string
	fmt.Println("enter command:")
	_, err := fmt.Scanln(&input)
	if err != nil {
		return
	}

	cmd, err := frontend.Parse(input)

	fmt.Println(cmd)
	fmt.Println(err)
}
