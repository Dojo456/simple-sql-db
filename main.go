package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/Dojo456/simple-sql-db/engine"
	"log"
	"os"
)

func main() {
	defer os.Exit(0)
	defer fmt.Println("\ngracefully shutting down")

	ctx := context.Background()

	scanner := bufio.NewScanner(os.Stdin)
	sqlEngine, err := engine.New(ctx)
	if err != nil {
		log.Fatalln(fmt.Errorf("could not intialize SQL Engine: %w", err))
	}

	defer func(sqlEngine *engine.SQLEngine) {
		err := sqlEngine.Cleanup()
		if err != nil {
			log.Println(fmt.Errorf("SQL engine could not properly cleanup: %w", err))
		}
	}(sqlEngine)

	for {
		fmt.Println("\nenter command:")

		// Scan() will return false on errors
		if !scanner.Scan() {
			fmt.Println(fmt.Errorf("error reading input: %w", scanner.Err()))

			return
		}
		input := scanner.Text()

		cmd, err := sqlEngine.Execute(ctx, input)
		if err != nil {
			fmt.Println(fmt.Errorf("error executing command: %w", err))
			continue
		}

		fmt.Printf("\n%v", cmd)
	}
}
