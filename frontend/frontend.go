package frontend

import (
	"fmt"
	"runtime"
	"strings"
)

// keyword defines the list of keywords that the database cli supports
type keyword string

const (
	putCommand keyword = "PUT"
	getCommand keyword = "GET"
	endCommand keyword = "END"
)

func (k keyword) IsValid() bool {
	switch k {
	case putCommand, getCommand, endCommand:
		return true
	}
	return false
}

func (k keyword) ArgsRequired() int {
	switch k {
	case putCommand:
		return 2
	case getCommand:
		return 1
	case endCommand:
		return 0
	}

	return 0
}

// Command represents a callable command in the database execution engine
type Command struct {
	Keyword keyword
	Args    []string
}

func Parse(commandString string) (*Command, error) {
	tokens := strings.Split(commandString, " ")

	if isEmptyString(commandString) {
		return nil, fmt.Errorf("empty command")
	}
	keyword := keyword(tokens[0])
	if !keyword.IsValid() {
		return nil, fmt.Errorf("%s is not a valid keyword", keyword)
	}

	// subtract 1 to account for command itself
	argsSupplied := len(tokens) - 1
	argsExpected := keyword.ArgsRequired()

	if argsSupplied != argsExpected {
		return nil, fmt.Errorf("wrong number of arguments: %d expected, %d supplied", argsExpected, argsSupplied)
	}

	parsedCommand := Command{
		Keyword: keyword,
		Args:    tokens[1:],
	}

	return &parsedCommand, nil
}

func Execute(cmd *Command) (interface{}, error) {
	if cmd == nil {
		return nil, fmt.Errorf("nil command")
	}

	switch cmd.Keyword {
	case endCommand:
		runtime.Goexit()
	}

	return nil, nil
}
