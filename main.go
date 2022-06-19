package main

import (
	"bufio"
	"fmt"
	"github.com/Dojo456/simple-inmem-db/frontend"
	"os"
)

func main() {
	defer os.Exit(0)
	defer fmt.Println("\ngracefully shutting down")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println("\nenter command:")

		// Scan() will return false on errors
		if !scanner.Scan() {
			fmt.Println(fmt.Errorf("error reading input: %w", scanner.Err()))

			return
		}
		input := scanner.Text()

		cmd, err := frontend.Parse(input)
		if err != nil {
			fmt.Println(fmt.Errorf("error parsing command: %w", err))
			continue
		}

		fmt.Println(cmd)

		_, err = frontend.Execute(cmd)
		if err != nil {
			fmt.Println(fmt.Errorf("error executing command: %w", err))
			continue
		}
	}
}
