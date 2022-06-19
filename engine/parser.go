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

	}

	return nil
}

func (k keyword) tokensNeeded() int {
	switch k {
	case CreateKeyword:
		return 2
	}

	return 0
}
