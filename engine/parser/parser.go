package parser

import (
	"fmt"
	"strings"
)

type keyword string

const (
	KeywordSelect keyword = "select"
	KeywordFrom   keyword = "from"
	KeywordAs     keyword = "as"
	KeywordTable  keyword = "table"
	KeywordCreate keyword = "create"
	KeywordInsert keyword = "insert"
	KeywordInto   keyword = "into"
	KeywordValues keyword = "values"
	KeywordDelete keyword = "delete"
	KeywordUpdate keyword = "update"
	KeywordSet    keyword = "set"
	KeywordWhere  keyword = "where"
)

var separators = map[rune]bool{' ': true, '\n': true}

func split(statement string) ([]string, error) {
	var tokens []string

	var currentToken = strings.Builder{}

	for i := 0; i < len(statement); i++ {
		r := rune(statement[i])

		if _, has := separators[r]; has {
			tokens = append(tokens, currentToken.String())
			currentToken = strings.Builder{}
		} else {
			currentToken.WriteRune(r)
		}
	}

	tokens = append(tokens, currentToken.String())

	return tokens, nil
}

func Validate(statement string) error {
	tokens, err := split(statement)
	if err != nil {
		return err
	}

	fmt.Println(tokens)

	return nil
}
