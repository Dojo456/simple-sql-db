package backend

import (
	"bufio"
	"context"
	"fmt"
	"io"
)

// InsertRow adds a new row to the table with the given Values. It will attempt to parse the Values into the
// correct primitive type, if it is unable to do so, an error will be returned. It returns the number of rows written
func (t *table) InsertRow(ctx context.Context, values []Value) (int, error) {
	fields := t.Fields

	if len(fields) < len(values) {
		return 0, fmt.Errorf("there are only %d fields on this table", len(t.Fields))
	}

	valsMap := make(map[string]Value, len(values))
	for _, val := range values {
		valsMap[val.FieldName] = val
	}

	// a buffer is not needed as the total size is fixed
	b := make([]byte, 0, t.rowByteCount)

	for _, field := range fields {
		var bytes []byte

		val, exists := valsMap[field.Name]
		if exists {
			bytes = val.Bytes()
		} else {
			bytes = make([]byte, field.Type.Size())
		}

		b = append(b, bytes...)
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

// Row is a Value slice alongside with the file system index of the row.
type Row struct {
	Values []Value
	index  int64
}

// rowsThatMatch returns an array of rows that match the specified filter. This should be used as the implementation
// of the WHERE clause for any statements that support one. If the filter is nil, all rows will be selected.
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

	// begin file operations
	t.mrw.RLock()
	defer t.mrw.RUnlock()

	_, err := t.file.Seek(t.headerByteCount, 0)
	if err != nil {
		return nil, fmt.Errorf("could not skip table header: %w", err)
	}

	reader := bufio.NewReader(t.file)

	returner := make([]Row, 0, t.rowCount)

	var i int64 = 0
	for ; i < t.rowCount; i++ {
		row := make([]Value, 0, len(t.Fields))
		shouldAddRow := true

		var cursor int64 = 0
		rowBytes := make([]byte, t.rowByteCount)
		_, err := io.ReadFull(reader, rowBytes)
		if err != nil {
			return nil, fmt.Errorf("could not read row %d: %w", i, err)
		}

		for _, field := range t.Fields {
			if shouldAddRow {
				isFilterField := false
				if filter != nil {
					isFilterField = field.Name == filter.FieldName
				}

				// read cell
				cellBytes := rowBytes[cursor : cursor+field.Type.Size()]
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
					Type:      field.Type,
					Val:       cell,
					FieldName: field.Name,
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

// GetRows returns the selected fields from a table that matches the filter. If fields is a zero length slice, all
// fields will be returned. If the filter is nil, all rows will be returned.
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

// DeleteRows deletes all rows that match the filter. If the filter is nil, all rows will be deleted. It returns the
// number of rows deleted.
func (t *table) DeleteRows(ctx context.Context, filter *Filter) (int, error) {
	// delete all rows
	if filter == nil {
		t.mrw.Lock()
		defer t.mrw.Unlock()

		file := t.file

		_, err := file.Seek(0, 0)
		if err != nil {
			return 0, fmt.Errorf("could not seek before truncate: %w", err)
		}

		err = file.Truncate(t.headerByteCount)
		if err != nil {
			return 0, fmt.Errorf("could not truncate: %w", err)
		}

		affected := t.rowCount

		t.rowCount = 0
		t.fileByteCount = t.headerByteCount

		return int(affected), nil
	}

	rows, err := t.rowsThatMatch(ctx, filter)
	if err != nil {
		return 0, err
	}

	if len(rows) == 0 {
		return 0, nil
	}

	type chunk struct {
		start int64
		end   int64
		shift int64
	}

	// each chunk represents a collection of rows that all need to be shifted by the same amount
	chunks := make([]chunk, 0, len(rows))

	var start *int64
	for i := 0; i < len(rows); i++ {
		row := rows[i]

		if start == nil {
			temp := row.index + 1
			start = &temp
		}

		var end int64

		// if last row
		if i == len(rows)-1 {
			end = t.rowCount - 1
		} else {
			// check for consecutive rows
			next := rows[i+1]
			if next.index == row.index+1 {
				start = nil
				continue
			} else {
				end = next.index - 1
			}
		}

		chunks = append(chunks, chunk{
			start: *start,
			end:   end,
			shift: int64(i + 1),
		})
	}

	// each chunk is shifted together, so need to ensure not too much data
	// is loaded into mem at once
	maxChunkSize := int64(1048576) // 1MB

	temp := make([]chunk, 0, len(chunks))
	for _, current := range chunks {
		rowCount := current.end - current.start + 1
		chunkSize := rowCount * t.rowByteCount

		// if split needed
		if chunkSize > maxChunkSize && rowCount > 2 {
			split := (current.end + current.start) / 2

			temp = append(temp, chunk{
				start: current.start,
				end:   split,
				shift: current.shift,
			})

			temp = append(temp, chunk{
				start: split + 1,
				end:   current.end,
				shift: current.shift,
			})
		}
	}

	t.mrw.Lock()
	defer t.mrw.Unlock()

	file := t.file

	for _, chunk := range chunks {
		rowCount := chunk.end - chunk.start + 1
		chunkSize := rowCount * t.rowByteCount

		// number of bytes before the first byte of the chunk
		offset := (chunk.start * t.rowByteCount) + t.headerByteCount

		chunkBytes := make([]byte, chunkSize)
		_, err := file.ReadAt(chunkBytes, offset)
		if err != nil {
			return 0, err
		}

		_, err = file.WriteAt(chunkBytes, offset-(chunk.shift*t.rowByteCount))
		if err != nil {
			return 0, err
		}
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		return 0, fmt.Errorf("could not seek before truncate: %w", err)
	}

	bytesToTruncate := int64(len(rows)) * t.rowByteCount

	err = file.Truncate(t.fileByteCount - bytesToTruncate)
	if err != nil {
		return 0, fmt.Errorf("could not truncate: %w", err)
	}

	t.rowCount -= int64(len(rows))
	t.fileByteCount -= bytesToTruncate

	return len(rows), nil
}

// UpdateRows updates all rows that match the filter to have the provided values. If the filter is nil, all rows will
// be updated. It returns the number of rows that had a value changed. Meaning, if a row matches the filter but did
// not require an update, it will not count towards the return value.
func (t *table) UpdateRows(ctx context.Context, values []Value, filter *Filter) (int, error) {
	oldRows, err := t.rowsThatMatch(ctx, filter)
	if err != nil {
		return 0, err
	}

	valsMap := make(map[string]Value, len(values))
	for _, val := range values {
		valsMap[val.FieldName] = val
	}

	type rowBytes struct {
		index int64
		bytes []byte
	}

	newRows := make([]rowBytes, 0, len(oldRows))
	for _, oldRow := range oldRows {
		requiresUpdate := false
		newRow := make([]byte, 0, t.rowByteCount)

		for j, field := range t.Fields {
			oldVal := oldRow.Values[j]

			if newVal, exists := valsMap[field.Name]; exists {
				if newVal.Val != oldVal.Val {
					requiresUpdate = true
				}

				newRow = append(newRow, newVal.Bytes()...)
			} else {
				newRow = append(newRow, oldVal.Bytes()...)
			}
		}

		if requiresUpdate {
			newRows = append(newRows, rowBytes{index: oldRow.index, bytes: newRow})
		}
	}

	t.mrw.Lock()
	defer t.mrw.Unlock()

	file := t.file
	for _, row := range newRows {
		offset := (row.index * t.rowByteCount) + t.headerByteCount

		_, err := file.WriteAt(row.bytes, offset)
		if err != nil {
			return 0, fmt.Errorf("could not update row %d: %w", row.index, err)
		}
	}

	return len(newRows), nil
}
