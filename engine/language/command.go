package language

import (
	"fmt"
	"strings"

	"github.com/Dojo456/simple-sql-db/backend"
)

// Command is an executable SQL Command that requires 0 or more arguments afterwards
type Command int

const (
	CreateTableCommand Command = iota
	SelectCommand
	InsertCommand
	DeleteCommand
	UpdateCommand
)

func getCommand(keywords []keyword) (*Command, error) {
	if len(keywords) == 0 {
		return nil, nil
	}

	var returner Command
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
	case KeywordDelete:
		{
			if len(keywords) > 1 {
				second := keywords[1]
				switch second {
				case KeywordFrom:
					returner = DeleteCommand
					found = true
				}
			}
		}
	case KeywordUpdate:
		returner = UpdateCommand
		found = true
	}

	if !found {
		return nil, nil
	}

	return &returner, nil
}

// UntypedValue is an unparsed value that has the potential to be parsed into a backend.Value.
type UntypedValue struct {
	Val       string
	FieldName string
}

type CreateTableArgs struct {
	TableName string
	Fields    []backend.Field
}

type SelectArgs struct {
	TableName  string
	FieldNames []string
	AllFields  bool
	Filter     *WhereClause
}

type InsertArgs struct {
	TableName     string
	Values        []UntypedValue
	HasFieldNames bool
}

type DeleteArgs struct {
	TableName string
	Filter    *WhereClause
}

type UpdateArgs struct {
	TableName string
	Values    []UntypedValue
	Filter    *WhereClause
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
	tokensUsed := 0
	name := truncated[tokensUsed]
	if name.t != TokenTypeValue {
		return nil, tokensUsed, fmt.Errorf("invalid syntax")
	}
	tokensUsed++

	// either fields specifier or values
	fieldsOrValue := truncated[tokensUsed]
	hasFieldNames := false
	var fieldNames []string
	if fieldsOrValue.t == TokenTypeParenthesisGroup { // values are explicitly assigned to fields
		fieldNames = strings.Split(fieldsOrValue.s, ",")

		// validate that each field is only specified once
		visited := make(map[string]bool, len(fieldNames))
		for _, sField := range fieldNames {
			if visited[sField] {
				return nil, tokensUsed, fmt.Errorf("cannot insert into same field twice: %s", sField)
			}

			visited[sField] = true
		}

		hasFieldNames = true
		tokensUsed++
	}

	valKeyword := truncated[tokensUsed]
	if valKeyword.t != TokenTypeValue || asKeyword(valKeyword.s) != KeywordValues {
		return nil, tokensUsed, fmt.Errorf("expecting VALUES keyword")
	}
	tokensUsed++

	valuesToken := truncated[tokensUsed]
	if valuesToken.t != TokenTypeParenthesisGroup {
		return nil, tokensUsed, fmt.Errorf("invalid values syntax")
	}
	tokensUsed++

	valueStrings := strings.Split(cleanString(valuesToken.s), ",")

	if !hasFieldNames {
		fieldNames = make([]string, len(valueStrings))
	}

	strippedNames := make([]string, len(fieldNames))
	for i, fieldName := range fieldNames {
		strippedNames[i] = stripTableNameFromField(fieldName, name.s)
	}

	var values []UntypedValue
	for i, value := range valueStrings {
		values = append(values, UntypedValue{
			Val:       value,
			FieldName: strippedNames[i],
		})
	}

	return &InsertArgs{TableName: name.s, Values: values, HasFieldNames: hasFieldNames}, tokensUsed, nil
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

	strippedNames := make([]string, len(fieldNames))
	for i, fieldName := range fieldNames {
		strippedNames[i] = stripTableNameFromField(fieldName, name)
	}

	return &SelectArgs{
		TableName:  name,
		FieldNames: strippedNames,
		AllFields:  allFields,
		Filter:     whereClause,
	}, tokensUsed, nil
}

func captureDeleteArgs(truncated []token) (*DeleteArgs, int, error) {
	if len(truncated) < 1 {
		return nil, 0, fmt.Errorf("not enough arguments")
	}

	tokensUsed := 0

	name := truncated[tokensUsed]
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

	var valTokens [][]token

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

	// values
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
			FieldName: stripTableNameFromField(e1.s, name.s),
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

func searchWhereClause(truncated []token, tokensUsed int) (*WhereClause, int, error) {
	if tokensUsed != len(truncated) && keyword(strings.ToLower(truncated[tokensUsed].s)) == KeywordWhere {
		if len(truncated) < tokensUsed+3 {
			return nil, 0, fmt.Errorf("incomplete WHERE clause")
		}

		tokensUsed++
		wT := truncated[tokensUsed : tokensUsed+3]

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
