package backend

import (
	"context"
	"fmt"
)

// InsertRow adds a new row to the table with the given values. It will attempt to parse the values into the
// correct primitive type, if it is unable to do so, an error will be returned. It returns the number of rows written
func (t *Table) InsertRow(ctx context.Context, vals []string) (int, error) {
	t.mrw.Lock()
	defer t.mrw.Unlock()

	fields := t.Fields

	if len(fields) != len(vals) {
		return 0, fmt.Errorf("values must have a length of %d", len(t.Fields))
	}

	// a buffer is not needed as the total size is known
	b := make([]byte, 0, t.rowByteCount)

	for i := 0; i < len(fields); i++ {
		s := vals[i]
		field := fields[i]

		var val value
		var err error

		switch field.Type {
		case stringPrimitive:
			val, err = stringValue(s)
		case intPrimitive:
			val, err = intValue(s)
		case floatPrimitive:
			val, err = floatValue(s)
		}

		if err != nil {
			return 0, fmt.Errorf("%s.%s must be of type %s", t.Name, field.Name, string(field.Type))
		}

		b = append(b, val.Bytes()...)
	}

	// reslice b to be full length since each row must be of the same size
	b = b[:cap(b)]

	file := t.file
	n, err := file.WriteAt(b, t.fileByteCount)
	if err != nil {
		return 0, fmt.Errorf("could not write to file: %w", err)
	}

	t.fileByteCount += int64(n)

	return 1, nil
}

// GetAllRows returns a two-dimensional string slice which represents all data within a table. Each cell is formatted
// into a string use the standard format for string, int64, and float64.
func (t *Table) GetAllRows(ctx context.Context) ([][]string, error) {
	t.mrw.RLock()
	defer t.mrw.RUnlock()

	dataByteCount := t.fileByteCount - t.headerByteCount

	dataBytes := make([]byte, dataByteCount)

	t.file.ReadAt(dataBytes, t.headerByteCount)

	return nil, nil
}
