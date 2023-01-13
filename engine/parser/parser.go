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

func isKeyword(s string) bool {
	k := asKeyword(s)

	return k.IsValid()
}

// asKeyword turns a string into a keyword, ignoring case. This is preferred over calling keyword(s).
func asKeyword(s string) keyword {
	s = strings.ToLower(s)

	return keyword(s)
}

func (k keyword) IsValid() bool {
	switch k {
	case KeywordSelect, KeywordFrom, KeywordAs, KeywordTable, KeywordCreate, KeywordInsert, KeywordInto, KeywordValues, KeywordWhere, KeywordDelete, KeywordUpdate, KeywordSet:
		return true
	}
	return false
}

type symbol string

const (
	symbolSemicolon   symbol = ";"
	symbolAsterisk    symbol = "*"
	symbolComma       symbol = ","
	symbolLeftParen   symbol = "("
	symbolRightParen  symbol = ")"
	symbolEqual       symbol = "="
	symbolGreaterThan symbol = "<"
	symbolLessThan    symbol = ">"
	symbolNot         symbol = "!"
)

func isSymbol(r rune) bool {
	sym := symbol(r)

	return sym.IsValid()
}

func (s symbol) IsValid() bool {
	switch s {
	case symbolSemicolon, symbolAsterisk, symbolComma, symbolLeftParen, symbolRightParen, symbolEqual, symbolGreaterThan, symbolLessThan, symbolNot:
		return true
	}
	return false
}

type tokenType int

const (
	TokenTypeValue tokenType = iota
	TokenTypeParenthesisGroup
	TokenTypeQuoteGroup
	TokenTypeKeyword
	TokenTypeSymbolGroup
)

type token struct {
	s string
	t tokenType
}

// split parses a SQL string and converts it to a token slice. Important to note that
// keywords CANNOT be identified using this function, and another pass through the tokens
// must be done to do syntax and semantic checking.
func split(statementString string) ([]token, error) {
	if isEmptyString(statementString) {
		return nil, fmt.Errorf("empty command")
	}

	statementString = cleanString(statementString)

	var currentToken strings.Builder
	var tokens []token

	addCurrentToken := func() {
		if currentToken.Len() != 0 {
			toString := currentToken.String()

			tokens = append(tokens, token{
				s: toString,
				t: TokenTypeValue,
			})
			currentToken = strings.Builder{}
		}
	}

	// using iterator loop to allow for easier of movement of cursor (incrementing or decrementing i)
	for i := 0; i < len(statementString); i++ {
		r := rune(statementString[i])

		// TODO look into pattern matching switch statements in Go
		if r == '(' { // special parenthesis group
			addCurrentToken()

			group, end, err := captureParenthesisGroup(statementString, i)
			if err != nil {
				return nil, fmt.Errorf("could not capture parenthesis group at %d: %w", i, err)
			}

			tokens = append(tokens, token{
				s: group,
				t: TokenTypeParenthesisGroup,
			})
			i = end
			continue
		} else if isQuote(r) {
			addCurrentToken()

			group, end, err := captureQuoteGroup(statementString, i)
			if err != nil {
				return nil, fmt.Errorf("could not capture quote group at %d: %w", i, err)
			}

			tokens = append(tokens, token{
				s: group,
				t: TokenTypeQuoteGroup,
			})
			i = end
			continue
		} else if isSymbol(r) { // special case of symbol group, usually for operators in WHERE clause
			addCurrentToken()

			group, end, err := captureSymbolGroup(statementString, i)
			if err != nil {
				return nil, fmt.Errorf("could not capture symbol group at %d: %w", i, err)
			}

			tokens = append(tokens, token{
				s: group,
				t: TokenTypeSymbolGroup,
			})
			i = end
			continue
		} else if r == ' ' { // found end to current token, begin next token
			addCurrentToken()
		} else { // adding to current token
			currentToken.WriteRune(r)
			continue
		}
	}

	// in case there is an end token
	addCurrentToken()

	return tokens, nil
}

// captureParenthesisGroup captures the parenthesis group starting at the given index in the given string.
// It is an error if the rune at the start index is not an open parenthesis.
// It returns the entire group as one string, excluding the outermost parenthesis, and also returns the rune index of
// the closing parenthesis.
func captureParenthesisGroup(s string, start int) (group string, end int, err error) {
	if s[start] != '(' {
		return "", 0, fmt.Errorf("starting rune is not an open parenthesis")
	}
	var captured strings.Builder

	i := start + 1
	closed := false

	for ; i < len(s); i++ {
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

// captureQuoteGroup captures a group surrounded by either single or double quotes starting at the given index in the
// given string.
// It is an error if the rune at the start index is not either a single or double quote.
// It returns the entire group as one string, excluding the enclosing quotes, and also the rune index of the closing
// quote.
func captureQuoteGroup(s string, start int) (group string, end int, err error) {
	if !isQuote(rune(s[start])) {
		return "", 0, fmt.Errorf("starting rune is not a quote")
	}
	var captured strings.Builder

	i := start + 1
	closed := false

	for ; i < len(s); i++ {
		r := rune(s[i])

		if isQuote(r) {
			closed = true
			break
		}

		captured.WriteRune(r)
	}

	if !closed {
		return "", 0, fmt.Errorf("unclosed quotes")
	}

	return captured.String(), i, nil
}

func isQuote(r rune) bool {
	return r == '\'' || r == '"'
}

func captureSymbolGroup(s string, start int) (group string, end int, err error) {
	if !isSymbol(rune(s[start])) {
		return "", 0, fmt.Errorf("starting rune is not a symbol")
	}
	var captured strings.Builder

	i := start

	for ; i < len(s); i++ {
		r := rune(s[i])

		if !isSymbol(r) {
			i--
			break
		}

		captured.WriteRune(r)
	}

	return captured.String(), i, nil
}
