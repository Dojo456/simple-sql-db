package engine

import (
	"context"
	"fmt"
	"github.com/Dojo456/simple-sql-db/backend"
	"strings"
)

func (e *SQLEngine) createTable(ctx context.Context, args []interface{}) (*backend.Table, error) {
	// two args are needed, first the name then the fields as a parenthesis group
	if len(args) != 2 {
		return nil, fmt.Errorf("wrong number of arguments for CreateTable: expected 2 args, received %d", len(args))
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

func parseTableFields(s string) ([]*backend.Field, error) {
	// the cleaned string should have the outer parenthesis removed and no newlines or redundant whitespaces
	// fields in a CREATE TABLE statement are separated by commas
	s = cleanString(s)

	rawFields := strings.Split(s, ",")
	parsedFields := make([]*backend.Field, len(rawFields))

	for i, rf := range rawFields {
		f, err := parseField(rf)
		if err != nil {
			return nil, fmt.Errorf("could not parse field %d: %w", i, err)
		}

		parsedFields[i] = f
	}

	return parsedFields, nil
}

func parseField(s string) (*backend.Field, error) {
	// name and data type are separated by space per field
	tokens := strings.Split(s, " ")
	if len(tokens) != 2 {
		return nil, fmt.Errorf("%s is not acceptable", s)
	}

	name := tokens[0]
	dataType := backend.Primitive(strings.ToLower(tokens[1]))

	if !dataType.IsValid() {
		return nil, fmt.Errorf("%s is not a valid data type", dataType)
	}

	return &backend.Field{
		Name: name,
		Type: dataType,
	}, nil
}

func (e *SQLEngine) insertRow(ctx context.Context, args []interface{}) (int, error) {
	name, ok := args[0].(string)
	if !ok {
		return 0, fmt.Errorf("name of table must be string")
	}

	// values is a string of values separated by commas
	values := strings.Split(args[2].(string), ",")

	table, err := e.getTable(ctx, name)
	if err != nil {
		return 0, fmt.Errorf("could not open table file: %w", err)
	}

	count, err := table.InsertRow(ctx, values)
	if err != nil {
		return 0, fmt.Errorf("could not open insert row: %w", err)
	}

	return count, nil
}

func (e *SQLEngine) getRows(ctx context.Context, name string) ([][]string, error) {
	t, err := e.getTable(ctx, name)
	if err != nil {
		return nil, err
	}

	rows, err := t.GetAllRows(ctx)
	if err != nil {
		return nil, err
	}

	returner := make([][]string, len(rows))

	for i := 0; i < len(rows); i++ {
		row := make([]string, len(t.Fields))

		for j, cell := range rows[i] {
			var cellString string

			switch v := cell.Val.(type) {
			case string:
				cellString = v
			case int64:
				cellString = fmt.Sprintf("%d", v)
			case float64:
				cellString = fmt.Sprintf("%.4f", v)
			}

			row[j] = cellString
		}

		returner[i] = row
	}

	return returner, nil
}

func (e *SQLEngine) getTable(ctx context.Context, name string) (*backend.Table, error) {
	var err error

	table, open := e.openTables[name]
	if !open {
		table, err = backend.OpenTable(name)
		if err != nil {
			return nil, err
		}

		e.openTables[name] = table
	}

	return table, nil
}
