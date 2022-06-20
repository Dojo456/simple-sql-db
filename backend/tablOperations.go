package backend

import "fmt"

// InsertRow adds a new row to the table with the given values. It returns the number of rows written
func (t *Table) InsertRow(vals []*Value) (int, error) {
	t.mrw.Lock()
	defer t.mrw.Unlock()

	fields := t.Fields

	if len(fields) != len(vals) {
		return 0, fmt.Errorf("values must have a length of %d", len(t.Fields))
	}

	// a buffer is not needed as the total size is known
	b := make([]byte, 0, t.rowByteCount)

	for i := 0; i < len(fields); i++ {
		val := vals[i]
		field := fields[i]

		if val.Type != field.Type {
			return 0, fmt.Errorf("%s.%s must be of type %s", t.Name, field.Name, string(field.Type))
		}

		b = append(b, val.Bytes()...)
	}

	file := t.file
	n, err := file.WriteAt(b, t.fileSize)
	if err != nil {
		return 0, fmt.Errorf("could not write to file: %w", err)
	}

	t.fileSize += int64(n)

	return 1, nil
}
