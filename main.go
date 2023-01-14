package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Dojo456/simple-sql-db/engine"
)

func main() {
	ctx := context.Background()

	scanner := bufio.NewScanner(os.Stdin)
	sqlEngine, err := engine.New(ctx)
	if err != nil {
		log.Fatalln(fmt.Errorf("could not intialize SQL Engine: %w", err))
	}

	defer cleanup(sqlEngine)
	go func() {
		sigchan := make(chan os.Signal)
		signal.Notify(sigchan, os.Interrupt)
		<-sigchan
	}()

	for {
		fmt.Println("\nenter command:")

		// Scan() will return false on errors
		if !scanner.Scan() {
			fmt.Println(fmt.Errorf("error reading input: %w", scanner.Err()))

			return
		}
		input := scanner.Text()

		cmd, err := sqlEngine.Process(ctx, input)
		if err != nil {
			fmt.Println(fmt.Errorf("\nerror executing command: %w", err))
			continue
		}

		fmt.Printf("\n%v\n", cmd)
	}
}

func cleanup(e *engine.SQLEngine) {
	fmt.Println("\ngracefully shutting down")

	err := e.Cleanup()
	if err != nil {
		fmt.Println("could not perform clean shutdown", err)
	} else {
		fmt.Println("shutdown complete")
	}

	os.Exit(0)
}
