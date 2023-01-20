package engine

import (
	"context"

	"github.com/Dojo456/simple-sql-db/backend"
	"github.com/Dojo456/simple-sql-db/engine/language"
)

func filterFromWhereClause(whereClause *language.WhereClause, table backend.OperableTable) (*backend.Filter, error) {
	if whereClause == nil {
		return nil, nil
	}

	var filter *backend.Filter

	field, err := table.FieldWithName(whereClause.FieldName)
	if err != nil {
		return nil, err
	}

	// value name is fifth argument
	value, err := language.NewValueForField(field, whereClause.UntypedValue.Val)
	if err != nil {
		return nil, err
	}

	filter = &backend.Filter{
		FieldName: field.Name,
		Operator:  whereClause.Operator,
		Value:     value,
	}

	return filter, nil
}

func selectRows(ctx context.Context, t backend.OperableTable, fieldsToSelect []string, whereClause *language.WhereClause, filters []backend.Filter) ([]backend.Row, error) {
	filter, err := filterFromWhereClause(whereClause, t)
	if err != nil {
		return nil, err
	}

	if filter != nil {
		filters = append(filters, *filter)
	}

	rows, err := t.GetRows(ctx, fieldsToSelect, filters)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func contains[T comparable](slice []T, element T) bool {
	for _, t := range slice {
		if t == element {
			return true
		}
	}

	return false
}
