package language

import (
	"fmt"
	"strings"

	"github.com/Dojo456/simple-sql-db/backend"
)

func Validate(statement string) error {
	_, err := split(statement)
	if err != nil {
		return fmt.Errorf("could not split statement: %w", err)
	}

	return nil
}

// Parse takes the SQL statement and returns an executable command the arguments for that command. If err is nil, then command
// is guaranteed to be not-nil.
func Parse(statement string) (cmd *Command, args interface{}, err error) {
	tokens, err := split(statement)
	if err != nil {
		return nil, nil, fmt.Errorf("could not split statement: %w", err)
	}

	var currentKeywords []keyword

	for i := 0; i < len(tokens); i++ {
		token := tokens[i]

		if isKeyword(token.s) {
			currentKeywords = append(currentKeywords, asKeyword(token.s))
		}

		cmd, err = getCommand(currentKeywords)
		if err != nil {
			return nil, nil, fmt.Errorf("could not get command: %w", err)
		}

		if cmd != nil { // an executable command has been found
			var end int
			args, end, err = captureArguments(*cmd, tokens, i)
			if err != nil {
				var mapped []string
				for _, k := range currentKeywords {
					mapped = append(mapped, string(k))
				}

				joined := strings.Join(mapped, " ")

				return nil, nil, fmt.Errorf("invalid arguements for %s statement: %w", joined, err)
			}

			i = end
		}
	}

	return
}

// split parses a SQL string and converts it to a token slice.
func split(statement string) ([]token, error) {
	if isEmptyString(statement) {
		return nil, fmt.Errorf("empty command")
	}

	statement = cleanString(statement)

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
	for i := 0; i < len(statement); i++ {
		r := rune(statement[i])

		// TODO look into pattern matching switch statements in Go
		if r == '(' { // special parenthesis group
			addCurrentToken()

			group, end, err := captureParenthesisGroup(statement, i)
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

			group, end, err := captureQuoteGroup(statement, i)
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

			group, end, err := captureSymbolGroup(statement, i)
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

// captureArguments will capture all arguments required for an executable from the list of tokens with the start index
// being the index of the last token in the command statement. If arguments cannot be properly captured, an error
// will be returned. It returns the arguments as an evaluable slice and the index of the last argument token.
func captureArguments(forCommand Command, tokens []token, start int) (args interface{}, end int, err error) {
	truncated := tokens[start+1:]
	index := 0

	switch forCommand {
	case CreateTableCommand:
		args, index, err = captureCreateTableArgs(truncated)
	case InsertCommand:
		args, index, err = captureInsertArgs(truncated)
	case SelectCommand:
		args, index, err = captureSelectArgs(truncated)
	case DeleteCommand:
		args, index, err = captureDeleteArgs(truncated)
	case UpdateCommand:
		args, index, err = captureUpdateArgs(truncated)
	}

	if err != nil {
		return nil, 0, err
	}

	return args, start + index, nil
}

func captureCreateTableArgs(truncated []token) (*CreateTableArgs, int, error) {
	if len(truncated) < 2 {
		return nil, 0, fmt.Errorf("not enough arguments")
	}

	i := 0
	name := truncated[i]
	if name.t != TokenTypeValue {
		return nil, i, fmt.Errorf("invalid table name")
	}
	i++

	fields := truncated[i]
	if fields.t != TokenTypeParenthesisGroup {
		return nil, i, fmt.Errorf("invalid fields declaration")
	}
	i++

	// the cleaned string should have the outer parenthesis removed and no newlines or redundant white spaces
	// fields in a CREATE TABLE statement are separated by commas
	s := cleanString(fields.s)

	rawFields := strings.Split(s, ",")
	parsedFields := make([]backend.Field, len(rawFields))

	for j, rf := range rawFields {
		f, err := parseField(rf)
		if err != nil {
			return nil, i, fmt.Errorf("could not parse field %d: %w", j, err)
		}

		parsedFields[j] = f
	}

	return &CreateTableArgs{TableName: name.s, Fields: parsedFields}, i, nil
}

func captureInsertArgs(truncated []token) (*InsertArgs, int, error) {
	if len(truncated) < 3 {
		return nil, 0, fmt.Errorf("not enough arguments")
	}

	// table name
	i := 0
	name := truncated[i]
	if name.t != TokenTypeValue {
		return nil, i, fmt.Errorf("invalid syntax")
	}

	// either fields specifier or values
	fieldsOrValue := truncated[i]
	hasFieldNames := false
	var fieldNames []string
	if fieldsOrValue.t == TokenTypeParenthesisGroup { // values are explicitly assigned to fields
		fieldNames = strings.Split(fieldsOrValue.s, ",")

		// validate that each field is only specified once
		visited := make(map[string]bool, len(fieldNames))
		for _, sField := range fieldNames {
			if visited[sField] {
				return nil, i, fmt.Errorf("cannot insert into same field twice: %s", sField)
			}

			visited[sField] = true
		}

		hasFieldNames = true
		i++
	}

	valKeyword := truncated[i]
	if valKeyword.t != TokenTypeValue || asKeyword(valKeyword.s) != KeywordValues {
		return nil, i, fmt.Errorf("expecting VALUE keyword")
	}
	i++

	valuesToken := truncated[i]
	if valuesToken.t != TokenTypeParenthesisGroup {
		return nil, i, fmt.Errorf("invalid values syntax")
	}
	valueStrings := strings.Split(valuesToken.s, ",")

	if len(fieldNames) == 0 {
		fieldNames = make([]string, len(valueStrings))
	}

	var values []UntypedValue
	for i, value := range valueStrings {
		values = append(values, UntypedValue{
			Val:       value,
			FieldName: fieldNames[i],
		})
	}

	return &InsertArgs{TableName: name.s, Values: values, HasFieldNames: hasFieldNames}, i, nil
}

func captureSelectArgs(truncated []token) (*SelectArgs, int, error) {
	tokensUsed := 0

	var fieldNames []string
	for l := len(truncated); tokensUsed < l; tokensUsed++ {
		c := truncated[tokensUsed]
		if strings.ToLower(c.s) == string(KeywordFrom) {
			break
		}

		split := strings.Split(c.s, ",")

		fieldNames = append(fieldNames, split...)
	}
	tokensUsed++

	// check for * select all
	allFields := len(fieldNames) == 1 && fieldNames[0] == "*"
	if allFields {
		fieldNames = nil
	}

	name := truncated[tokensUsed].s
	tokensUsed++

	// search for WHERE clause
	whereClause, temp, err := searchWhereClause(truncated, tokensUsed)
	if err != nil {
		return nil, 0, fmt.Errorf("could not parse WHERE clause: %w", err)
	}
	tokensUsed += temp

	return &SelectArgs{
		TableName:  name,
		FieldNames: fieldNames,
		AllFields:  allFields,
		Filter:     whereClause,
	}, tokensUsed, nil
}

func captureDeleteArgs(truncated []token) (*DeleteArgs, int, error) {
	if len(truncated) < 1 {
		return nil, 0, fmt.Errorf("not enough arguments")
	}

	tokensUsed := 0

	name := truncated[0]
	if name.t != TokenTypeValue {
		return nil, 0, fmt.Errorf("invalid table name")
	}
	tokensUsed++

	// search for WHERE clause
	whereClause, temp, err := searchWhereClause(truncated, tokensUsed)
	if err != nil {
		return nil, 0, fmt.Errorf("could not parse WHERE clause: %w", err)
	}
	tokensUsed += temp

	return &DeleteArgs{
		TableName: name.s,
		Filter:    whereClause,
	}, tokensUsed, nil
}

func captureUpdateArgs(truncated []token) (*UpdateArgs, int, error) {
	if len(truncated) < 3 {
		return nil, 0, fmt.Errorf("not enough arguments")
	}

	// table name
	tokensUsed := 0
	name := truncated[tokensUsed]
	if name.t != TokenTypeValue {
		return nil, 0, fmt.Errorf("invalid syntax")
	}
	tokensUsed++

	// SET keyword
	setKeyword := truncated[tokensUsed]
	if setKeyword.t != TokenTypeValue || asKeyword(setKeyword.s) != KeywordSet {
		return nil, 0, fmt.Errorf("invalid syntax")
	}
	tokensUsed++

	// capture Values and break on WHERE clause
	var valTokens [][]token // each value is a

	i := 0
	for ; i+tokensUsed < len(truncated); i++ {
		current := truncated[i+tokensUsed]

		if asKeyword(current.s) == KeywordWhere { // found WHERE clause and all values have been captured
			break
		}

		// floored
		valIndex := i / 3
		if valIndex >= len(valTokens) {
			valTokens = append(valTokens, make([]token, 0, 3))
		}

		valTokens[valIndex] = append(valTokens[valIndex], current)
	}
	tokensUsed += i

	var vals []UntypedValue
	for i, tokens := range valTokens {
		e1, op, e2, err := parseEquation(tokens)
		if err != nil {
			return nil, 0, fmt.Errorf("could not parse value number %d: %w", i+1, err)
		}

		if op != backend.OperatorEqual {
			return nil, 0, fmt.Errorf("value statement must use \"=\" between field name and value")
		}

		vals = append(vals, UntypedValue{
			Val:       e2.s,
			FieldName: e1.s,
		})
	}

	whereClause, temp, err := searchWhereClause(truncated, tokensUsed)
	if err != nil {
		return nil, 0, fmt.Errorf("could not parse WHERE clause: %w", err)
	}
	tokensUsed += temp

	return &UpdateArgs{
		TableName: name.s,
		Values:    vals,
		Filter:    whereClause,
	}, tokensUsed, nil
}

type WhereClause struct {
	UntypedValue
	Operator backend.Operator
}

func (c *WhereClause) Filter(t backend.OperableTable) (backend.Filter, error) {
	field, err := t.FieldWithName(c.FieldName)
	if err != nil {
		return backend.Filter{}, err
	}

	val, err := field.NewValue(c.Val)
	if err != nil {
		return backend.Filter{}, err
	}

	return backend.Filter{
		Value:     val,
		Operator:  c.Operator,
		FieldName: c.FieldName,
	}, nil
}

func searchWhereClause(truncated []token, tokensUsed int) (*WhereClause, int, error) {
	if tokensUsed != len(truncated) && keyword(strings.ToLower(truncated[tokensUsed].s)) == KeywordWhere {
		if len(truncated) < tokensUsed+3 {
			return nil, 0, fmt.Errorf("incomplete WHERE clause")
		}

		tokensUsed++
		stringTokens := truncated[tokensUsed : tokensUsed+3]

		wT := make([]token, 3)
		for j := 0; j < 2; j++ {
			wT[j] = token{
				s: stringTokens[j].s,
				t: TokenTypeValue,
			}
		}

		valueTokenString := stringTokens[2].s
		valueTokenType := TokenTypeValue

		if isQuote(rune(valueTokenString[0])) && isQuote(rune(valueTokenString[len(valueTokenString)-1])) {
			valueTokenType = TokenTypeQuoteGroup
		}

		wT[2] = token{
			s: valueTokenString,
			t: valueTokenType,
		}

		// parse field name
		fieldNameToken := wT[0]
		if fieldNameToken.t != TokenTypeValue {
			return nil, 0, fmt.Errorf("could not parse WHERE clause")
		}

		// parse operator
		operatorToken := wT[1]
		if fieldNameToken.t != TokenTypeValue {
			return nil, 0, fmt.Errorf("could not parse WHERE clause")
		}
		operator := backend.Operator(operatorToken.s)
		if !operator.IsValid() {
			return nil, 0, fmt.Errorf("%s is not a valid operator", operatorToken.s)
		}

		// parse valueString to compare to
		valueToken := wT[2]
		tokensUsed += 3

		return &WhereClause{
			UntypedValue: UntypedValue{
				Val:       valueToken.s,
				FieldName: fieldNameToken.s,
			},
			Operator: operator,
		}, tokensUsed, nil
	}

	return nil, 0, nil
}

// parseWhereClause parses a WHERE clause given in the form of a token slice of len 3 and returns a
// slice with length 3 of those fields parsed. It is a wrapper over parseEquation to accommodate
// a common use case of that function as the parser of a WHERE clause.
func parseWhereClause(tokens []token) (WhereClause, error) {
	e1, op, e2, err := parseEquation(tokens)
	if err != nil {
		return WhereClause{}, err
	}

	return WhereClause{UntypedValue{
		Val:       e2.s,
		FieldName: e1.s,
	}, op}, nil
}

// parseEquation parses a statement in token slice of len 3. It returns the three
// respective elements of the equation.
func parseEquation(tokens []token) (e1 token, operator backend.Operator, e2 token, err error) {
	if len(tokens) < 3 {
		return token{}, "", token{}, fmt.Errorf("not enough arguments")
	} else if len(tokens) != 3 {
		return token{}, "", token{}, fmt.Errorf("invalid syntax")
	}

	// parse field name
	fieldNameToken := tokens[0]
	if fieldNameToken.t != TokenTypeValue {
		return token{}, "", token{}, fmt.Errorf("invalid first operand")
	}

	// parse operator
	operatorToken := tokens[1]
	op := backend.Operator(operatorToken.s)
	if !op.IsValid() {
		return token{}, "", token{}, fmt.Errorf("%s is not a valid operator", operatorToken.s)
	}

	// parse valueString to compare to
	valueToken := tokens[2]
	if valueToken.t != TokenTypeValue && valueToken.t != TokenTypeQuoteGroup {
		return token{}, "", token{}, fmt.Errorf("invalid value to compare to")
	}

	return tokens[0], backend.Operator(tokens[1].s), tokens[2], nil
}
