package language

import "strings"

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
