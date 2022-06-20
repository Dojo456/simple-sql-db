package engine

import (
	"context"
	"fmt"
	"github.com/Dojo456/simple-sql-db/backend"
	"log"
	"runtime/debug"
	"strings"
)

type SQLEngine struct {
	openTables map[string]*backend.Table
}

type Cleanable interface {
	Cleanup() error
}

// New returns a new engine instance that can then be used to execute SQL statements.
func New(ctx context.Context) (*SQLEngine, error) {
	return &SQLEngine{
		map[string]*backend.Table{},
	}, nil
}

// Execute parses then executes the given statement string. It will return a value if the executed statement requires
// one. Else, the return value is nil.
func (e *SQLEngine) Execute(ctx context.Context, statement string) (interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("\nEngine panic recovered", r)
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

		if isKeyword(token.s) {
			currentKeywords = append(currentKeywords, asKeyword(token.s))
		}

		cmd, err := getCommand(currentKeywords)
		if err != nil {
			return nil, fmt.Errorf("could not get command: %w", err)
		}

		if cmd != nil { // an executable command has been found
			args, end, err := cmd.captureArguments(tokens, i)
			if err != nil {
				var mapped []string
				for _, k := range currentKeywords {
					mapped = append(mapped, string(k))
				}

				joined := strings.Join(mapped, " ")

				return nil, fmt.Errorf("invalid arguements for %s statement: %w", joined, err)
			}

			exec = &executable{
				Cmd:  *cmd,
				Args: args,
			}

			i = end
		}
	}

	if exec == nil {
		return nil, fmt.Errorf("not evaluable")
	}

	return exec.Value(ctx, e)
}

func (e *SQLEngine) Cleanup() error {
	for _, table := range e.openTables {
		err := table.Cleanup()
		if err != nil {
			return err
		}
	}

	return nil
}
