// Package backend The backend package interacts with storage devices to provide the data storage functionality of the
// database.
package backend

import (
	"context"
	"encoding/gob"
	"fmt"
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

// CreateTable creates a table and returns the table corresponding table struct.
func CreateTable(ctx context.Context, name string, fields []*Field) (*Table, error) {
	path := fmt.Sprintf("./%s-db", name)

	file, err := getFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not open db file: %w", err)
	}

	table := Table{
		Name:   name,
		Fields: fields,
	}

	fmt.Println("creating table")
	fmt.Println(table)

	enc := gob.NewEncoder(file)

	// write the table struct to the file to act as a table and schema
	err = enc.Encode(table)
	if err != nil {
		return nil, fmt.Errorf("could not encode table metadata: %w", err)
	}

	err = file.Close()
	if err != nil {
		return nil, fmt.Errorf("could not close file: %w", err)
	}

	return &table, nil
}
