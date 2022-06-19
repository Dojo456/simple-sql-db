package engine

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
)

type SQLEngine struct {
}

// New returns a new engine instance that can then be used to execute SQL statements.
func New(ctx context.Context) (*SQLEngine, error) {
	return &SQLEngine{}, nil
}

// Execute parses then executes the given statement string. It will return a value if the executed statement requires
// one. Else, the return value is nil.
func (e SQLEngine) Execute(ctx context.Context, statement string) (interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("\nEngine panic recovered: %w", r)
			debug.PrintStack()
		}
	}()

	tokens, err := Parse(statement)
	if err != nil {
		return nil, fmt.Errorf("could not parse statement: %w", err)
	}

	var currentKeywords []keyword
	var exec *executable

	for i := 0; i < len(tokens); i++ {
		token := tokens[i]

		asAKeyword := keyword(strings.ToLower(token))
		if asAKeyword.IsValid() {
			currentKeywords = append(currentKeywords, asAKeyword)
		}

		cmd, err := getCommand(currentKeywords)
		if err != nil {
			return nil, fmt.Errorf("could not get command: %w", err)
		}

		if cmd != nil { // an executable command has been found
			next := cmd.argsNeeded()
			i++
			argString := tokens[i : i+next]
			i += next

			args := make([]*evaluable, len(argString))
			for i, as := range argString {
				val := evaluable(value{val: as})

				args[i] = &val
			}

			exec = &executable{
				Cmd:  *cmd,
				Args: args,
			}
		}
	}

	if exec == nil {
		return nil, fmt.Errorf("not evaluable")
	}

	return exec.Value(ctx)
}
