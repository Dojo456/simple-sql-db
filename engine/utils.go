package engine

import (
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
	value, err := field.NewValue(whereClause.UntypedValue.Val)
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
