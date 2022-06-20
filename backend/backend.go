// Package backend The backend package interacts with storage devices to provide the data storage functionality of the
// database.
package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// Primitive represents all the data types that the database can store.
type Primitive string

const (
	StringPrimitive Primitive = "string" // 256 runes max (1024 bytes)
	IntPrimitive    Primitive = "int"    // int64 (8 bytes)
	FloatPrimitive  Primitive = "float"  // float64 (8 bytes)
)

func (p Primitive) IsValid() bool {
	switch p {
	case StringPrimitive, IntPrimitive, FloatPrimitive:
		return true
	}

	return false
}

func (p Primitive) Size() uint64 {
	switch p {
	case StringPrimitive:
		return 1024
	case IntPrimitive, FloatPrimitive:
		return 8
	}

	return 0
}

// Value is a single cell in a table. It allows type-safe operations between Go primitives and DB primitives.
//
// Do not create using struct literal, instead use either StringValue, IntValue, or FloatValue.
type Value struct {
	Type Primitive
	Val  interface{}
}

// StringValue returns a new value struct with Type StringPrimitive. If the provided string is longer than the max string
// length (256 runes), the string will be truncated.
func StringValue(s string) (*Value, error) {
	if len(s) > 256 {
		s = s[:256]
	}

	return &Value{
		Type: StringPrimitive,
		Val:  s,
	}, nil
}

func (v *Value) Bytes() []byte {
	switch val := v.Val.(type) {
	case string:
		return []byte(val)
	case int64:
		return i64tob(uint64(val))
	case float64:
		return f64tob(val)
	}

	return nil
}

// Field is essentially a column in a table.
type Field struct {
	Name string
	Type Primitive
}

type Table struct {
	mrw             *sync.RWMutex
	file            *os.File
	fileSize        int64
	headerByteCount uint64
	rowByteCount    uint64
	rowCount        uint64
	Name            string
	Fields          []*Field
}

func (t *Table) Cleanup() error {
	return t.file.Close()
}

func getTableFilePath(name string) string {
	return fmt.Sprintf("./database/%s-db", name)
}

// CreateTable creates a table and returns the table corresponding table struct.
func CreateTable(ctx context.Context, name string, fields []*Field) (*Table, error) {
	path := getTableFilePath(name)

	file, err := createFile(path)

	if err != nil {
		if errors.Is(err, fileAlreadyExistsError) {
			return nil, fmt.Errorf(`table with name "%s" already exists`, name)
		} else {
			return nil, fmt.Errorf("could not create table db file: %w", err)
		}
	}

	table := Table{
		rowCount:     0,
		file:         file,
		Name:         name,
		Fields:       fields,
		rowByteCount: calculateRowSize(fields),
	}

	fmt.Println("creating table: ")
	fmt.Println(table)

	// write the table struct to the file to act as a table and schema
	err = table.writeTableHeader()
	if err != nil {
		return nil, fmt.Errorf("could not write table header: %w", err)
	}

	err = file.Close()
	if err != nil {
		return nil, fmt.Errorf("could not close file: %w", err)
	}

	return &table, nil
}

// writeTableHeader writes the header to initialize a table file. It will flush a table if there is already data.
func (t *Table) writeTableHeader() error {
	t.mrw.Lock()
	defer t.mrw.Unlock()

	data, err := json.Marshal(*t)
	if err != nil {
		return fmt.Errorf("could not encode table metadata: %w", err)
	}

	headerByteCount := 4 + len(data)

	// begin header with an unsigned i32 that is the number of bytes of the table size, including the number itself
	header := i64tob(uint64(headerByteCount))
	header = append(header, data...)

	_, err = t.file.Write(header)
	if err != nil {
		return err
	}

	t.headerByteCount = uint64(headerByteCount)

	return nil
}

// calculateRowSize calculates the numbers of bytes each row of the table takes. This should be called on table
// initialization and stored into the Table struct.
func calculateRowSize(fields []*Field) uint64 {
	var sum uint64

	for _, field := range fields {
		sum += field.Type.Size()
	}

	return sum
}

// readTableFile reads a tableFile's header to create a Table struct that can then be used for operations.
func readTableFile(file *os.File) (*Table, error) {
	headerSizeBytes := make([]byte, 4)

	_, err := file.Read(headerSizeBytes)
	if err != nil {
		return nil, err
	}

	headerSize := btoi64(headerSizeBytes)
	header := make([]byte, headerSize)

	_, err = file.Read(header)
	if err != nil {
		return nil, err
	}

	// first four bytes are the table header size
	data := header[4:]

	var table *Table
	err = json.Unmarshal(data, table)
	if err != nil {
		return nil, err
	}

	table.headerByteCount = headerSize
	table.file = file
	table.rowByteCount = calculateRowSize(table.Fields)

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("could not open file stats: %w", err)
	}

	table.fileSize = stat.Size()

	return table, nil
}

// OpenTable returns a Table struct that tableReader and tableWriters can attach to. If table with given name does not
// exist, an error will be returned. An opened Table needs to be cleaned up later through the Cleanable interface
func OpenTable(name string) (*Table, error) {
	path := getTableFilePath(name)

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("table with name %s does not exist", name)
		} else {
			return nil, fmt.Errorf("could not open table file: %w", err)
		}
	}

	return readTableFile(f)
}
