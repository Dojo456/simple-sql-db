package engine

import (
	"fmt"
	"github.com/Dojo456/simple-sql-db/backend"
	"regexp"
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
	case KeywordSelect, KeywordFrom, KeywordAs, KeywordTable, KeywordCreate, KeywordInsert, KeywordInto, KeywordValues, KeywordWhere:
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

// Parse parses a SQL string and converts it to a token slice.
func Parse(statementString string) ([]token, error) {
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

// captureUntilKeyword captures everything until the next keyword after the given start index. If the keyword cannot be
// found, an error is returned.
func captureUntilKeyword(s string, k keyword, start int) (group string, end int, err error) {
	truncated := s[start:]

	expr, err := regexp.Compile(fmt.Sprintf(".+(?=%s)", k))
	if err != nil {
		return "", 0, err
	}

	group = expr.FindString(truncated)
	if group == "" {
		return "", 0, fmt.Errorf("could not find %s keyword", k)
	}

	return
}

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
	case KeywordCreate:
		{
			if len(keywords) > 1 {
				second := keywords[1]
				switch second {
				case KeywordTable:
					returner = CreateTableCommand
					found = true
				}
			}
		}
	case KeywordSelect:
		returner = SelectCommand
		found = true
	case KeywordInsert:
		{
			if len(keywords) > 1 {
				second := keywords[1]
				switch second {
				case KeywordInto:
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
// will be returned. It returns the arguments as an evaluable slice and the index of the last argument token.
func (c command) captureArguments(tokens []token, start int) (args []evaluable, end int, err error) {
	truncated := tokens[start+1:]
	argsCaptured := 0

	switch c {
	case CreateTableCommand:
		args, argsCaptured, err = captureCreateTableArgs(truncated)
	case InsertCommand:
		args, argsCaptured, err = captureInsertArgs(truncated)
	case SelectCommand:
		args, argsCaptured, err = captureSelectArgs(truncated)
	}

	if err != nil {
		return nil, 0, err
	}

	return args, start + argsCaptured, nil
}

func captureCreateTableArgs(truncated []token) ([]evaluable, int, error) {
	if len(truncated) < 2 {
		return nil, 0, fmt.Errorf("not enough arguments")
	}

	name := truncated[0]
	if name.t != TokenTypeValue {
		return nil, 0, fmt.Errorf("invalid table name")
	}

	fields := truncated[1]
	if fields.t != TokenTypeParenthesisGroup {
		return nil, 0, fmt.Errorf("invalid fields declaration")
	}

	return []evaluable{
		asValue(name.s),
		asValue(fields.s),
	}, 2, nil
}

func captureInsertArgs(truncated []token) ([]evaluable, int, error) {
	if len(truncated) < 3 {
		return nil, 0, fmt.Errorf("not enough arguments")
	}

	args := make([]evaluable, 0, 3)

	name := truncated[0]
	if name.t != TokenTypeValue {
		return nil, 0, fmt.Errorf("invalid table name")
	}
	args = append(args, asValue(name.s))

	//fieldsOrValue := truncated[1]
	//
	//if fieldsOrValue.t == TokenTypeParenthesisGroup { // values are explicitly assigned to fields
	//
	//}

	valKeyword := truncated[1]
	if valKeyword.t != TokenTypeValue || asKeyword(valKeyword.s) != KeywordValues {
		return nil, 0, fmt.Errorf("invalid syntax")
	}
	args = append(args, asValue(valKeyword.s))

	values := truncated[2]
	if values.t != TokenTypeParenthesisGroup {
		return nil, 0, fmt.Errorf("invalid values syntax")
	}
	args = append(args, asValue(values.s))

	return args, 3, nil
}

func captureSelectArgs(truncated []token) ([]evaluable, int, error) {
	var args []evaluable

	i := 0

	for l := len(truncated); i < l; i++ {
		c := truncated[i]
		if strings.ToLower(c.s) == string(KeywordFrom) {
			// search for WHERE clause
			break
		}

		split := strings.Split(c.s, ",")

		for _, t := range split {
			args = append(args, value{
				val: t,
			})
		}
	}

	i++
	name := truncated[i].s
	args = append(args, value{val: name})

	i++
	// search for WHERE clause
	if i != len(truncated) && keyword(strings.ToLower(truncated[i].s)) == KeywordWhere {
		var wT []token

		if len(truncated) == i+4 { // in format WHERE field = value
			wT = truncated[i:]
		} else { // in format WHERE field=value
			stringTokens := strings.Split(truncated[i+1].s, "=")

			wT := make([]token, 3)
			for j := 0; j < 2; j++ {
				wT[i] = token{
					s: stringTokens[i],
					t: TokenTypeValue,
				}
			}

			valueTokenString := stringTokens[2]
			valueTokenType := TokenTypeValue

			if isQuote(rune(valueTokenString[0])) && isQuote(rune(valueTokenString[len(valueTokenString)-1])) {
				valueTokenType = TokenTypeQuoteGroup
			}

			wT[2] = token{
				s: valueTokenString,
				t: valueTokenType,
			}
		}

		// parse field name
		fieldNameToken := wT[0]
		if fieldNameToken.t != TokenTypeValue {
			return nil, 0, fmt.Errorf("could not parse WHERE clause")
		}
		args = append(args, value{val: fieldNameToken.s})

		// parse operator
		operatorToken := wT[1]
		if fieldNameToken.t != TokenTypeValue {
			return nil, 0, fmt.Errorf("could not parse WHERE clause")
		}
		operator := backend.Operator(operatorToken.s)
		if !operator.IsValid() {
			return nil, 0, fmt.Errorf("%s is not a valid operator", operatorToken.s)
		}
		args = append(args, value{val: operator})

		// parse valueString to compare to
		valueToken := wT[2]
		args = append(args, value{val: valueToken.s})
	}

	// add 1 as the FROM keyword is not an argument but is still captured
	return args, len(args) + 1, nil
}
