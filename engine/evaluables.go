package engine

import (
	"context"
	"fmt"
)

// command is an executable SQL command that requires 0 or more arguments afterwards
type command int

const (
	CreateTableCommand command = iota
	SelectCommand
	InsertCommand
)

func getCommand(keywords []keyword) (*command, error) {
	if len(keywords) == 0 {
		return nil, nil
	}

	var returner command
	found := false

	first := keywords[0]
	switch first {
	case CreateKeyword:
		{
			if len(keywords) > 1 {
				second := keywords[1]
				switch second {
				case TableKeyword:
					returner = CreateTableCommand
					found = true
				}
			}
		}
	case SelectKeyword:
		returner = SelectCommand
		found = true
	case InsertKeyword:
		{
			if len(keywords) > 1 {
				second := keywords[1]
				switch second {
				case IntoKeyword:
					returner = InsertCommand
					found = true
				}
			}
		}

	}

	if !found {
		return nil, nil
	}

	return &returner, nil
}

// captureArguments will capture all arguments required for an executable from the list of tokens with the start index
// being the index of the last token in the command statement. If arguments cannot be properly captured, an error
// will be returned.
func (c command) captureArguments(tokens []string, start int) ([]*evaluable, error) {
	return nil, nil
}

// The execution engine works by building a tree of evaluables and then recursively evaluating all of them, starting
// from the bottom-most node which has no dependencies on other commands

// evaluable represents a statement that has a value. It can be either a value literal such as a string or an
// executable SQL statement
type evaluable interface {
	Value(ctx context.Context) (interface{}, error)
}

// toEvaluable parses a string and returns an evaluable which can be used to be build the evaluation tree. It will make
// either an executable or value based on the contents of the string
func toEvaluable(s string) (*evaluable, error) {
	return nil, nil
}

// executable is an SQL statement that can be executed to obtain a value. It implements the evaluable interface
type executable struct {
	Cmd  command
	Args []*evaluable
}

// Value recursively evaluates an executable down to the smallest executable that can be evaluated without
// any dependencies
func (e *executable) Value(ctx context.Context) (interface{}, error) {
	argValues := make([]interface{}, len(e.Args))
	for i, arg := range e.Args {
		val, err := (*arg).Value(ctx)
		if err != nil {
			return nil, err
		}

		argValues[i] = val
	}

	var returner interface{}

	switch e.Cmd {
	case CreateTableCommand:
		{
			_, err := createTable(ctx, argValues)
			if err != nil {
				return nil, fmt.Errorf("could not evaluate createTable: %w", err)
			}
		}
	}

	return returner, nil
}

// value is a raw value literal. It implements the evaluable interface
type value struct {
	val interface{}
}

func (v value) Value(ctx context.Context) (interface{}, error) {
	return v.val, nil
}
