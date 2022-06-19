// Package backend The backend package interacts with storage devices to provide the data storage functionality of the
// database.
package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Primitive represents all the data types that the database can store.
type Primitive string

const (
	StringPrimitive Primitive = "string"
	IntPrimitive    Primitive = "int"
	FloatPrimitive  Primitive = "float"
)

func (p Primitive) IsValid() bool {
	switch p {
	case StringPrimitive, IntPrimitive, FloatPrimitive:
		return true
	}

	return false
}

// Field is essentially a column in a table.
type Field struct {
	Name string
	Type Primitive
}

type Table struct {
	Name   string
	Fields []*Field
}

// HeaderSize is the number of bytes the table struct following is.
//
// Data is the json encoding of a Table struct to represent the schema
type tableHeader struct {
	HeaderSize uint32
	Data       []byte
}

// CreateTable creates a table and returns the table corresponding table struct.
func CreateTable(ctx context.Context, name string, fields []*Field) (*Table, error) {
	path := fmt.Sprintf("./database/%s-db", name)

	file, err := createFile(path)
	if err != nil {
		if errors.Is(err, fileAlreadyExistsError) {
			return nil, fmt.Errorf("table with name %s already exists", name)
		} else {
			return nil, fmt.Errorf("could not create table db file: %w", err)
		}
	}

	table := Table{
		Name:   name,
		Fields: fields,
	}

	fmt.Println("creating table: ")
	fmt.Println(table)

	// write the table struct to the file to act as a table and schema
	err = writeTableHeader(file, &table)
	if err != nil {
		return nil, fmt.Errorf("could not write table header: %w", err)
	}

	err = file.Close()
	if err != nil {
		return nil, fmt.Errorf("could not close file: %w", err)
	}

	return &table, nil
}

func writeTableHeader(file *os.File, table *Table) error {
	data, err := json.Marshal(table)
	if err != nil {
		return fmt.Errorf("could not encode table metadata: %w", err)
	}

	headerByteCount := 4 + len(data)

	// begin header with an unsigned i32 that is the number of bytes of the table size, including the number itself
	header := i32tob(uint32(headerByteCount))
	header = append(header, data...)

	_, err = file.Write(header)
	if err != nil {
		return err
	}

	return nil
}

func readTableHeader(file *os.File) (*Table, error) {
	headerSizeBytes := make([]byte, 4)

	_, err := file.Read(headerSizeBytes)
	if err != nil {
		return nil, err
	}

	headerSize := btoi32(headerSizeBytes)
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

	return table, nil
}
