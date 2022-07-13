package backend

import (
	"context"
	"fmt"
)

// Operator are the supported comparison operators in a WHERE clause of SELECT statement
type Operator string

const (
	OperatorEqual              Operator = "="
	OperatorNotEqual           Operator = "!="
	OperatorLessThan           Operator = "<"
	OperatorLessThanOrEqual    Operator = "<="
	OperatorGreaterThan        Operator = ">"
	OperatorGreaterThanOrEqual Operator = ">="
)

func (o Operator) IsValid() bool {
	switch o {
	case OperatorEqual, OperatorNotEqual, OperatorLessThan, OperatorLessThanOrEqual, OperatorGreaterThan, OperatorGreaterThanOrEqual:
		return true
	}

	return false
}

type Filter struct {
	Value
	FieldName string
	Operator  Operator
}

type Row struct {
	Values []Value
	index  int64
}

// rows that match returns the index
func (t *table) rowsThatMatch(ctx context.Context, filter *Filter) ([]Row, error) {

	// validate filter
	if filter != nil {
		field, err := t.FieldWithName(filter.FieldName)
		if err != nil {
			return nil, err
		}

		if field.Type != filter.Type {
			return nil, fmt.Errorf("%s.%s is of type %s", t.Name, field.Name, field.Type)
		}
	}

	// calculate byte offsets to begin file reading
	dataByteCount := t.fileByteCount - t.headerByteCount

	dataBytes := make([]byte, dataByteCount)

	// begin file operations
	t.mrw.RLock()
	defer t.mrw.RUnlock()

	n, err := t.file.ReadAt(dataBytes, t.headerByteCount)
	if err != nil {
		return nil, err
	}

	if int64(n) != dataByteCount {
		return nil, fmt.Errorf("corrupted file")
	}

	if t.rowByteCount*t.rowCount != int64(n) {
		return nil, fmt.Errorf("corrupted cache")
	}

	returner := make([]Row, 0, t.rowCount)

	var cursor int64 = 0
	var i int64 = 0
	for ; i < t.rowCount; i++ {
		row := make([]Value, 0, len(t.Fields))
		shouldAddRow := true

		for _, field := range t.Fields {
			if shouldAddRow {
				isFilterField := false
				if filter != nil {
					isFilterField = field.Name == filter.FieldName
				}

				// read cell
				cellBytes := dataBytes[cursor : cursor+field.Type.Size()]
				var cell interface{}
				switch field.Type {
				case PrimitiveString:
					cell = bToS(cellBytes)
				case PrimitiveFloat:
					cell = bToF64(cellBytes)
				case PrimitiveInt:
					cell = bToI64(cellBytes)
				}

				if isFilterField {
					switch c := cell.(type) {
					case string:
						shouldAddRow = compareValues(c, filter.Operator, filter.Val.(string))
					case int64:
						shouldAddRow = compareValues(c, filter.Operator, filter.Val.(int64))
					case float64:
						shouldAddRow = compareValues(c, filter.Operator, filter.Val.(float64))
					}
				}

				row = append(row, Value{
					Type: field.Type,
					Val:  cell,
				})
			}

			cursor += field.Type.Size()
		}

		if shouldAddRow {
			returner = append(returner, Row{
				Values: row,
				index:  i,
			})
		}
	}

	return returner, nil
}
