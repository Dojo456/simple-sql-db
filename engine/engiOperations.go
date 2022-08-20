package engine

import (
	"context"
	"fmt"
	"github.com/Dojo456/simple-sql-db/backend"
	"strconv"
	"strings"
)

func (e *SQLEngine) createTable(ctx context.Context, args []interface{}) (backend.OperableTable, error) {
	// two args are needed, first the name then the fields as a parenthesis group
	if len(args) != 2 {
		return nil, fmt.Errorf("expected 2 arguments, received %d", len(args))
	}

	name, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("name of table must be string")
	}

	fieldString, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("table fields must be written in string format")
	}

	fields, err := parseTableFields(fieldString)
	if err != nil {
		return nil, fmt.Errorf("could not parse table fields: %w", err)
	}

	table, err := backend.CreateTable(ctx, name, fields)
	if err != nil {
		return nil, fmt.Errorf("could not create table: %w", err)
	}

	e.openTables[name] = table

	return table, nil
}

func parseTableFields(s string) ([]backend.Field, error) {
	// the cleaned string should have the outer parenthesis removed and no newlines or redundant whitespaces
	// fields in a CREATE TABLE statement are separated by commas
	s = cleanString(s)

	rawFields := strings.Split(s, ",")
	parsedFields := make([]backend.Field, len(rawFields))

	for i, rf := range rawFields {
		f, err := parseField(rf)
		if err != nil {
			return nil, fmt.Errorf("could not parse field %d: %w", i, err)
		}

		parsedFields[i] = f
	}

	return parsedFields, nil
}

func parseField(s string) (backend.Field, error) {
	// name and data type are separated by space per field
	tokens := strings.Split(s, " ")
	if len(tokens) != 2 {
		return backend.Field{}, fmt.Errorf("%s is not acceptable", s)
	}

	name := tokens[0]
	dataType := backend.Primitive(strings.ToLower(tokens[1]))

	if !dataType.IsValid() {
		return backend.Field{}, fmt.Errorf("%s is not a valid data type", dataType)
	}

	return backend.Field{
		Name: name,
		Type: dataType,
	}, nil
}

// (name string, fieldNames CommaSeparateStringSlice, values CommaSeparatedStringSlice)
func (e *SQLEngine) insertRow(ctx context.Context, args []interface{}) (int, error) {
	name, ok := args[0].(string)
	if !ok {
		return 0, fmt.Errorf("name of table must be string")
	}

	// fieldNames is a string slice of values separated by commas
	var sFields []string
	if len(args[1].(string)) != 0 {
		sFields = strings.Split(args[1].(string), ",")
	}

	visited := make(map[string]bool, len(sFields))
	for _, sField := range sFields {
		if visited[sField] {
			return 0, fmt.Errorf("cannot insert into same field twice: %s", sField)
		}

		visited[sField] = true
	}

	// values is a string slice of values separated by commas
	sValues := strings.Split(args[2].(string), ",")

	table, err := e.getTable(ctx, name)
	if err != nil {
		return 0, fmt.Errorf("could not open table file: %w", err)
	}

	// fields to insert into in order
	var iFields []backend.Field

	tFields := table.GetFields()
	if len(sFields) == 0 { // fieldNames omitted, all fields are being inserted into in, in order
		if len(sValues) != len(tFields) {
			return 0, fmt.Errorf("mismatched number of values to fields: %d values, %d fields", len(sValues), len(tFields))
		}

		iFields = tFields
	} else {
		iFields = make([]backend.Field, len(sValues))

		for i, sField := range sFields {
			found := false

			for j, tField := range tFields {
				if tField.Name == sField {
					found = true
					tFields[j] = backend.Field{}
					iFields[i] = tField

					break
				}
			}

			if !found {
				return 0, fmt.Errorf("%s.%s does not exist", table.GetName(), sField)
			}
		}
	}

	values := make([]backend.Value, len(sValues))
	for i, sVal := range sValues {
		field := iFields[i]

		val, err := field.NewValue(sVal)
		if err != nil {
			return 0, fmt.Errorf("error with %s.%s: %w", table.GetName(), field.Name, err)
		}

		values[i] = val
	}

	count, err := table.InsertRow(ctx, values)
	if err != nil {
		return 0, fmt.Errorf("could not open insert row: %w", err)
	}

	return count, nil
}

// used to represent the arguments for a SELECT statement in SQL. If fields
// is of len zero or nil, then all fields will be fetched.
type tableQuery struct {
	tableName   string
	fields      []string
	whereClause *whereClause
}

func (e *SQLEngine) getTableRows(ctx context.Context, query tableQuery) ([][]string, error) {
	t, err := e.getTable(ctx, query.tableName)
	if err != nil {
		return nil, err
	}

	var filter *backend.Filter
	if query.whereClause != nil {
		temp, err := query.whereClause.Filter(t)
		if err != nil {
			return nil, err
		}

		filter = &temp
	}

	rows, err := t.GetRows(ctx, query.fields, filter)
	if err != nil {
		return nil, err
	}

	returner := make([][]string, len(rows))

	for i, valRow := range rows {
		values := valRow.Values

		row := make([]string, len(values))

		for j, cell := range values {
			var cellString string

			switch v := cell.Val.(type) {
			case string:
				cellString = fmt.Sprintf(`"%s"`, v)
			case int64:
				cellString = fmt.Sprintf("%d", v)
			case float64:
				cellString = strconv.FormatFloat(v, 'f', -1, 64)
			}

			row[j] = cellString
		}

		returner[i] = row
	}

	return returner, nil
}

func (e *SQLEngine) deleteRows(ctx context.Context, args []interface{}) (int, error) {
	if len(args) < 1 {
		return 0, fmt.Errorf("not enough arguments")
	}

	// name is first argument
	name := args[0].(string)

	t, err := e.getTable(ctx, name)
	if err != nil {
		return 0, err
	}

	// if there is a WHERE clause
	var filter *backend.Filter
	if len(args) > 1 {
		if len(args) < 4 {
			return 0, fmt.Errorf("not enough arguments for WHERE clause")
		}

		// field name is second argument
		fieldName := args[1].(string)

		// operator name is third argument
		operator := args[2].(backend.Operator)

		field, err := t.FieldWithName(fieldName)
		if err != nil {
			return 0, err
		}

		// value name is fourth argument
		value, err := field.NewValue(args[3])
		if err != nil {
			return 0, err
		}

		filter = &backend.Filter{
			FieldName: fieldName,
			Operator:  operator,
			Value:     value,
		}
	}

	n, err := t.DeleteRows(ctx, filter)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// (name string, values []untypedValue, filter *whereClause)
func (e *SQLEngine) updateRows(ctx context.Context, args []interface{}) (int, error) {
	if len(args) < 3 {
		return 0, fmt.Errorf("not enough arguments")
	}

	// name is first argument
	name := args[0].(string)

	t, err := e.getTable(ctx, name)
	if err != nil {
		return 0, err
	}

	// values is second argument
	rawVals := args[1].([]untypedValue)

	fieldsUsed := map[string]bool{}
	var vals []backend.Value

	for _, rawVal := range rawVals {
		if fieldsUsed[rawVal.FieldName] {
			return 0, fmt.Errorf("%s.%s cannot be SET twice", t.GetName(), rawVal.FieldName)
		}

		field, err := t.FieldWithName(rawVal.FieldName)
		if err != nil {
			return 0, fmt.Errorf("error with field %s.%s: %w", t.GetName(), rawVal.FieldName, err)
		}

		val, err := field.NewValue(rawVal.Val)
		if err != nil {
			return 0, fmt.Errorf("error with field %s.%s: %w", t.GetName(), rawVal.FieldName, err)
		}

		vals = append(vals, val)
	}

	// where clause is third argument
	where := args[2].(*whereClause)

	var filter *backend.Filter
	if where != nil {
		temp, err := where.Filter(t)
		if err != nil {
			return 0, fmt.Errorf("error with WHERE clause: %w", err)
		}

		filter = &temp
	}

	return t.UpdateRows(ctx, vals, filter)
}

func (e *SQLEngine) getTable(ctx context.Context, name string) (backend.OperableTable, error) {
	var err error

	table, open := e.openTables[name]
	if !open {
		table, err = backend.OpenTable(ctx, name)
		if err != nil {
			return nil, err
		}

		e.openTables[name] = table
	}

	return table, nil
}
