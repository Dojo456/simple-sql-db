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

func isKeyword(s string) bool {
	k := keyword(s)

	return k.IsValid()
}

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

func isSymbol(s string) bool {
	sym := symbol(s)

	return sym.IsValid()
}

func (s symbol) IsValid() bool {
	switch s {
	case semicolonSymbol, asteriskSymbol, commaSymbol, leftParenSymbol, rightParenSymbol:
		return true
	}
	return false
}

// Parse parses a SQL string and converts it to a list of tokens. A token is defined as an understandable SQL statement.
// e.g. (CREATE TABLE), (SELECT), or arguments to command.
func Parse(statementString string) ([]string, error) {
	if isEmptyString(statementString) {
		return nil, fmt.Errorf("empty command")
	}

	statementString = cleanString(statementString)

	var currentToken strings.Builder
	var tokens []string

	// using iterator loop to allow for easier of movement of cursor (incrementing or decrementing i)
	for i := 0; i < len(statementString); i++ {
		r := rune(statementString[i])

		// TODO look into pattern matching switch statements in Go
		if r == '(' {
			group, end, err := captureParenthesisGroup(i, statementString)
			if err != nil {
				return nil, fmt.Errorf("could not capture parenthesis group at %d: %w", i, err)
			}

			tokens = append(tokens, group)
			i = end
			continue
		} else if r == ' ' && currentToken.Len() != 0 {
			tokens = append(tokens, currentToken.String())
			currentToken = strings.Builder{}
		} else {
			currentToken.WriteRune(r)
			continue
		}
	}

	if currentToken.Len() != 0 {
		tokens = append(tokens, currentToken.String())
	}

	return tokens, nil
}

// captureParenthesisGroup captures the parenthesis group starting at the given index string.
// It is an error if the rune at the start index is not an open parenthesis.
// It returns the entire group as one string, excluding the outermost parenthesis,
// and also returns the rune index of the closing parenthesis.
func captureParenthesisGroup(start int, s string) (group string, end int, err error) {
	if s[start] != '(' {
		return "", 0, fmt.Errorf("starting rune is not an open parenthesis")
	}
	var captured strings.Builder

	i := start + 1
	closed := false

	for i = start + 1; i < len(s); i++ {
		c := rune(s[i])

		if c == ')' {
			closed = true
			break
		}

		captured.WriteRune(c)
	}

	if !closed {
		return "", 0, fmt.Errorf("unclosed parenthesis")
	}

	return captured.String(), i, nil
}
