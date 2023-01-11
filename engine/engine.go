package engine

import (
	"context"
	"log"
	"runtime/debug"

	"github.com/Dojo456/simple-sql-db/backend"
	"github.com/Dojo456/simple-sql-db/engine/parser"
)

type SQLEngine struct {
	openTables map[string]backend.OperableTable
}

type Cleanable interface {
	Cleanup() error
}

// New returns a new engine instance that can then be used to execute SQL statements.
func New(ctx context.Context) (*SQLEngine, error) {
	return &SQLEngine{
		map[string]backend.OperableTable{},
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

	err := parser.Validate(statement)
	if err != nil {
		return nil, err
	}

	return nil, err
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
