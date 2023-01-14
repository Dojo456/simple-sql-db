package engine

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"

	"github.com/Dojo456/simple-sql-db/backend"
	"github.com/Dojo456/simple-sql-db/engine/language"
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

// Process parses then executes the given statement string. Returned values are strings that are formatted.
func (e *SQLEngine) Process(ctx context.Context, statement string) (interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("\nEngine panic recovered", r)
			debug.PrintStack()
		}
	}()

	// syntax validation
	err := language.Validate(statement)
	if err != nil {
		return nil, err
	}

	cmd, args, err := language.Parse(statement)
	if err != nil {
		return nil, err
	}

	// semantic validation
	val, err := e.Execute(ctx, *cmd, args)
	if err != nil {
		return nil, err
	}

	return val, err
}

// Execute runs the given command with given args. It will return a value if the executed statement requires
// one. Else, the return value is nil.
func (e *SQLEngine) Execute(ctx context.Context, cmd language.Command, args interface{}) (interface{}, error) {
	switch cmd {
	case language.CreateTableCommand:
		return e.createTable(ctx, args.(*language.CreateTableArgs))
	case language.SelectCommand:
		return e.getRows(ctx, args.(*language.SelectArgs))
	case language.InsertCommand:
		return e.insertRow(ctx, args.(*language.InsertArgs))
	case language.DeleteCommand:
		return e.deleteRows(ctx, args.(*language.DeleteArgs))
	case language.UpdateCommand:
		return e.updateRows(ctx, args.(*language.UpdateArgs))
	}

	return nil, fmt.Errorf("invalid command")
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
