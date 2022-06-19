package engine

import (
	"fmt"
	"strings"
)

type keyword string

const (
	SelectKeyword keyword = "select"
	FromKeyword   keyword = "from"
	AsKeyword     keyword = "as"
	TableKeyword  keyword = "table"
	CreateKeyword keyword = "create"
	InsertKeyword keyword = "insert"
	IntoKeyword   keyword = "into"
	ValuesKeyword keyword = "values"
	IntKeyword    keyword = "int"
	TextKeyword   keyword = "text"
)

func (k keyword) IsValid() bool {
	switch k {
	case SelectKeyword, FromKeyword, AsKeyword, TableKeyword, CreateKeyword, InsertKeyword, IntoKeyword, ValuesKeyword, IntKeyword, TextKeyword:
		return true
	}
	return false
}

type symbol string

const (
	semicolonSymbol  symbol = ";"
	asteriskSymbol   symbol = "*"
	commaSymbol      symbol = ","
	leftParenSymbol  symbol = "("
	rightParenSymbol symbol = ")"
)

func (s symbol) IsValid() bool {
	switch s {
	case semicolonSymbol, asteriskSymbol, commaSymbol, leftParenSymbol, rightParenSymbol:
		return true
	}
	return false
}

type Statement struct {
	Keywords []keyword
	Args     []string
}

// Parse parses a SQL string and converts it to a format that can then be executed
func Parse(statementString string) error {
	tokens := strings.Split(statementString, " ")

	if isEmptyString(statementString) {
		return fmt.Errorf("empty command")
	}

	var currentKeywords []keyword

	for _, token := range tokens {
		asKeyword := keyword(token)

		if asKeyword.IsValid() {
			currentKeywords = append(currentKeywords, asKeyword)
		}

		exec, err := getExecutable(currentKeywords)
		if err != nil {
			return fmt.Errorf("malformed statement: %w", err)
		}

	}

	return nil
}

// executable represents a function argument pair that can actually be executed by the engine
type executable int

// None is a special case used in error handling
const (
	CreateTableExecutable executable = iota
	None
)

// Execute begins execution of a command and returns a value if the command invoked requires one
func Execute(statement *Statement) (interface{}, error) {
	if statement == nil {
		return nil, fmt.Errorf("cannot execute nil statement")
	}

	keywordsLeft := statement.Keywords
	argsLeft := statement.Args

	var currentKeywords []keyword

	// on each iteration, pops keywordsLeft and appends to currentKeywords
	for ; len(keywordsLeft) > 0; keywordsLeft = keywordsLeft[1:] {
		exec, err := getExecutable(currentKeywords)
		if err != nil {
			return nil, fmt.Errorf("could not get executable: %w", err)
		}

		if exec != nil { // Begin execution of command
			var arg string
			arg, argsLeft = argsLeft[0], argsLeft[1:]

		}

		currentKeywords = append(currentKeywords, keywordsLeft[0])
	}

}

// getExecutable gets an executable from a list of keywords. Most executables consists of 2 keywords.
// The function returns a pointer to an executable and an error so implementations can differentiate
// between statements with syntax errors or simple an incomplete statement
func getExecutable(keywords []keyword) (*executable, error) {
	if len(keywords) == 0 {
		return nil, nil
	}

	var returner executable

	first := keywords[0]
	switch first {
	case CreateKeyword:
		{
			if len(keywords) != 2 {
				return nil, nil
			}

			second := keywords[1]
			switch second {
			case TableKeyword:
				returner = CreateTableExecutable
			}
		}
	}

	return &returner, nil
}
