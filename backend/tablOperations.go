package backend

import (
	"context"
	"fmt"
)

// InsertRow adds a new row to the table with the given values. It will attempt to parse the values into the
// correct primitive type, if it is unable to do so, an error will be returned. It returns the number of rows written
func (t *Table) InsertRow(ctx context.Context, vals []Value) (int, error) {
	t.mrw.Lock()
	defer t.mrw.Unlock()

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

	file := t.file
	n, err := file.WriteAt(b, t.fileByteCount)
	if err != nil {
		return 0, fmt.Errorf("could not write to file: %w", err)
	}

	// increment cache values
	t.fileByteCount += int64(n)
	t.rowCount++

	return 1, nil
}

// GetAllRows returns the selected fields from a table in a two-dimensional string slice which represents all rows
// within a table.
func (t *Table) GetAllRows(ctx context.Context, fields []string) ([][]Value, error) {
	t.mrw.RLock()
	defer t.mrw.RUnlock()

	dataByteCount := t.fileByteCount - t.headerByteCount

	dataBytes := make([]byte, dataByteCount)

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

	returner := make([][]Value, t.rowCount)

	var cursor int64 = 0
	var i int64 = 0
	for ; i < t.rowCount; i++ {
		row := make([]Value, len(t.Fields))

		for j, field := range t.Fields {
			cellBytes := dataBytes[cursor : cursor+field.Type.Size()]

			var cell interface{}

			switch field.Type {
			case StringPrimitive:
				cell = bToS(cellBytes)
			case FloatPrimitive:
				cell = bToF64(cellBytes)
			case IntPrimitive:
				cell = bToI64(cellBytes)
			}

			row[j] = Value{
				Type: field.Type,
				Val:  cell,
			}

			cursor += field.Type.Size()
		}

		returner[i] = row
	}

	return returner, nil
}
