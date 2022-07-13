package backend

import (
	"context"
	"fmt"
)

// InsertRow adds a new row to the table with the given Values. It will attempt to parse the Values into the
// correct primitive type, if it is unable to do so, an error will be returned. It returns the number of rows written
func (t *table) InsertRow(ctx context.Context, vals []Value) (int, error) {
	fields := t.Fields

	if len(fields) < len(vals) {
		return 0, fmt.Errorf("there are only %d fields on this table", len(t.Fields))
	}

	// a buffer is not needed as the total size is known
	b := make([]byte, 0, t.rowByteCount)

	for _, val := range vals {
		b = append(b, val.Bytes()...)
	}

	// reslice b to be full length since each row must be of the same size
	b = b[:cap(b)]

	t.mrw.Lock()
	defer t.mrw.Unlock()

	file := t.file
	n, err := file.WriteAt(b, t.fileByteCount)
	if err != nil {
		return 0, fmt.Errorf("could not write to file: %w", err)
	}

	// increment cache Values
	t.fileByteCount += int64(n)
	t.rowCount++

	return 1, nil
}

// GetRows returns the selected fields from a table in a two-dimensional string slice which represents the rows within
// a table that satisfies the filter. If fields is a zero length slice, all fields will be returned. If the filter is
// nil, all rows will be returned.
func (t *table) GetRows(ctx context.Context, fields []string, filter *Filter) ([]Row, error) {
	shouldSelectField := make([]bool, len(t.Fields))
	fieldsToSelectCount := len(t.Fields)

	// if there is a filter for fields, validate field inputs
	if len(fields) != 0 {
		tFieldNames := make([]string, len(t.Fields))
		for i, field := range t.Fields {
			tFieldNames[i] = field.Name
		}

		for i, field := range tFieldNames {
			if contains[string](fields, field) {
				shouldSelectField[i] = true
				fieldsToSelectCount++
			} else {
				shouldSelectField[i] = false
			}
		}

		if fieldsToSelectCount != len(fields) {
			e := exclusive[string](fields, tFieldNames)[0]

			return nil, fmt.Errorf("%s.%s does not exist", t.Name, e)
		}
	} else {
		for i := 0; i < len(t.Fields); i++ {
			shouldSelectField[i] = true
		}

		fieldsToSelectCount = len(t.Fields)
	}

	rows, err := t.rowsThatMatch(ctx, filter)
	if err != nil {
		return nil, err
	}

	returner := make([]Row, len(rows))
	for i, row := range rows {
		filteredValues := make([]Value, 0, fieldsToSelectCount)

		for j, val := range row.Values {
			if shouldSelectField[j] {
				filteredValues = append(filteredValues, val)
			}
		}

		returner[i] = Row{Values: filteredValues, index: row.index}
	}

	return returner, nil
}
