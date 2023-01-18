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

			if !(group == "," && len(group) == 1) { // capturing actual symbol group
				tokens = append(tokens, token{
					s: group,
					t: TokenTypeSymbolGroup,
				})
			}

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
	escaped := false

	for ; i < len(s); i++ {
		r := rune(s[i])

		if escaped {
			captured.WriteRune(r)
			escaped = false
			continue
		}

		if r == '\\' { // escape character, take next character literally
			escaped = true
			continue
		}

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

func searchWhereClause(truncated []token, start int, tableName string) (*WhereClause, int, error) {
	if start != len(truncated) && keyword(strings.ToLower(truncated[start].s)) == KeywordWhere {
		tokensUsed := start

		if len(truncated) < tokensUsed+3 {
			return nil, 0, fmt.Errorf("incomplete WHERE clause")
		}

		tokensUsed++
		wT := truncated[tokensUsed : tokensUsed+3]

		// parse field name
		fieldNameToken := wT[0]
		if fieldNameToken.t != TokenTypeValue {
			return nil, tokensUsed, fmt.Errorf("could not parse WHERE clause")
		}
		fieldTableName, fieldName := asTableField(fieldNameToken.s)
		if !isEmptyString(fieldTableName) && fieldTableName != tableName {
			return nil, (tokensUsed - start), fmt.Errorf("field must be from table %s", tableName)
		}

		// parse operator
		operatorToken := wT[1]
		if fieldNameToken.t != TokenTypeValue {
			return nil, (tokensUsed - start), fmt.Errorf("could not parse WHERE clause")
		}
		operator := backend.Operator(operatorToken.s)
		if !operator.IsValid() {
			return nil, (tokensUsed - start), fmt.Errorf("%s is not a valid operator", operatorToken.s)
		}

		// parse valueString to compare to
		valueToken := wT[2]
		tokensUsed += 3

		return &WhereClause{
			UntypedValue: UntypedValue{
				Val:       valueToken.s,
				FieldName: fieldName,
			},
			Operator: operator,
		}, (tokensUsed - start), nil
	}

	return nil, 0, nil
}

func searchJoinClauses(truncated []token, start int, parentTable string) ([]JoinClause, int, error) {
	var clauses []JoinClause
	tokensUsed := start

	for tokensUsed <= len(truncated)-6 { // multiple join clauses can be made, so loop. each clause requires at least 7
		joinOrLoc := truncated[tokensUsed].s
		tokensUsed++

		location := JoinLocationInner

		if isJoinLocation(joinOrLoc) { // found a join clause with loc specifier
			s := truncated[tokensUsed].s
			tokensUsed++

			if asKeyword(s) != KeywordJoin {
				return nil, (tokensUsed - start), fmt.Errorf("expecting JOIN after join location specifier: %s", s)
			}

			location = asJoinLocation(joinOrLoc)
		} else if asKeyword(joinOrLoc) != KeywordJoin { // check for error scenario
			return nil, (tokensUsed - start), fmt.Errorf("expecting JOIN keyword, instead found %s", joinOrLoc)
		}

		tableName := truncated[tokensUsed].s
		tokensUsed++

		onKeyword := truncated[tokensUsed].s
		if asKeyword(onKeyword) != KeywordOn {
			return nil, (tokensUsed - start), fmt.Errorf("expecting ON keyword, instead found %s", onKeyword)
		}
		tokensUsed++

		e1, op, e2, err := parseEquation(truncated[tokensUsed : tokensUsed+3])
		if err != nil {
			return nil, (tokensUsed - start), fmt.Errorf("could not parse equation: %w", err)
		}
		tokensUsed += 3

		if op != backend.OperatorEqual {
			return nil, (tokensUsed - start), fmt.Errorf("equals (\"=\") is the only allowed operator in JOIN clauses")
		}

		tFields := []token{e1, e2}
		fields := map[string]string{}
		hasSpecifier := false

		for _, token := range tFields {
			tableFieldName, fieldName := asTableField(token.s)

			if isEmptyString(tableFieldName) {
				if _, exists := fields[parentTable]; exists {
					tableFieldName = tableName
				} else {
					tableFieldName = parentTable
				}
			} else {
				hasSpecifier = true
			}

			_, exists := fields[tableFieldName]
			if exists {
				return nil, (tokensUsed - start), fmt.Errorf("cannot specify two fields from the same table in JOIN clause")
			}

			fields[tableFieldName] = fieldName
		}
		if !hasSpecifier {
			return nil, (tokensUsed - start), fmt.Errorf("at least one field must specify table name in format of {tableName}.{fieldName}")
		}

		parentField, exists := fields[parentTable]
		if !exists {
			return nil, (tokensUsed - start), fmt.Errorf("at least one of the fields of ON should be from table %s", parentTable)
		}

		childField, exists := fields[tableName]
		if !exists {
			return nil, (tokensUsed - start), fmt.Errorf("at least one of the fields of ON should be from table %s", tableName)
		}

		whereClause, i, err := searchWhereClause(truncated, tokensUsed, tableName)
		if err != nil {
			return nil, (tokensUsed - start), err
		}
		tokensUsed += i

		clauses = append(clauses, JoinClause{
			ParentField: parentField,
			ChildField:  childField,
			TableName:   tableName,
			Location:    location,
			Filter:      whereClause,
		})
	}

	return clauses, (tokensUsed - start), nil
}
