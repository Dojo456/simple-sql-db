package engine

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Dojo456/simple-sql-db/backend"
	"github.com/Dojo456/simple-sql-db/engine/language"
)

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

func (e *SQLEngine) insertRow(ctx context.Context, args *language.InsertArgs) (int, error) {
	table, err := e.getTable(ctx, args.TableName)
	if err != nil {
		return 0, fmt.Errorf("could not open table file: %w", err)
	}

	// set of all fields in the table for quick checking
	tFields := map[string]backend.Field{}
	for _, field := range table.GetFields() {
		tFields[field.Name] = field
	}

	// fields to insert into in order
	var iFields []backend.Field

	if args.HasFieldNames {
		iFields = make([]backend.Field, len(args.Values))

		for _, uVal := range args.Values {
			field, exists := tFields[uVal.FieldName]

			if !exists {
				return 0, fmt.Errorf("%s.%s does not exist", table.GetName(), uVal.FieldName)
			}

			iFields = append(iFields, field)
		}
	} else { // fieldNames omitted, all fields are being inserted into in, in order
		iFields = table.GetFields()
	}

	values := make([]backend.Value, len(iFields))
	for i, uVal := range args.Values {
		field := iFields[i]

		val, err := field.NewValue(uVal)
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

func (e *SQLEngine) getRows(ctx context.Context, args *language.SelectArgs) ([][]string, error) {
	t, err := e.getTable(ctx, args.TableName)
	if err != nil {
		return nil, err
	}

	filter, err := filterFromWhereClause(args.Filter, t)
	if err != nil {
		return nil, err
	}

	var fieldsToSelect []string

	if args.AllFields {
		fieldsToSelect = nil
	} else {
		fieldsToSelect = args.FieldNames
	}

	rows, err := t.GetRows(ctx, fieldsToSelect, filter)
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

func (e *SQLEngine) deleteRows(ctx context.Context, args *language.DeleteArgs) (int, error) {
	t, err := e.getTable(ctx, args.TableName)
	if err != nil {
		return 0, err
	}

	filter, err := filterFromWhereClause(args.Filter, t)
	if err != nil {
		return 0, err
	}

	n, err := t.DeleteRows(ctx, filter)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (e *SQLEngine) updateRows(ctx context.Context, args *language.UpdateArgs) (int, error) {
	t, err := e.getTable(ctx, args.TableName)
	if err != nil {
		return 0, err
	}

	// values is second argument
	uVals := args.Values
	vals := make([]backend.Value, 0, len(uVals))

	for _, uVal := range uVals {
		field, err := t.FieldWithName(uVal.FieldName)
		if err != nil {
			return 0, fmt.Errorf("error with field %s.%s: %w", t.GetName(), uVal.FieldName, err)
		}

		val, err := field.NewValue(uVal.Val)
		if err != nil {
			return 0, fmt.Errorf("error with field %s.%s: %w", t.GetName(), uVal.FieldName, err)
		}

		vals = append(vals, val)
	}

	filter, err := filterFromWhereClause(args.Filter, t)
	if err != nil {
		return 0, err
	}

	return t.UpdateRows(ctx, vals, filter)
}

func (e *SQLEngine) createTable(ctx context.Context, args *language.CreateTableArgs) (backend.OperableTable, error) {
	name := args.TableName
	fields := args.Fields

	table, err := backend.CreateTable(ctx, name, fields)
	if err != nil {
		return nil, fmt.Errorf("could not create table: %w", err)
	}

	e.openTables[name] = table

	return table, nil
}
