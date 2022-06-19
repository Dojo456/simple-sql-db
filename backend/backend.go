// Package backend The backend package interacts with storage devices to provide the data storage functionality of the
// database.
package backend

import (
	"context"
	"fmt"
)

// Primitives represents all the data types that the database can store
type Primitives int

const (
	StringPrimitive Primitives = iota
	IntPrimitive
	FloatPrimitive
)

// Field is essentially a column in a table
type Field struct {
	Name string
	Type Primitives
}

type Table struct {
	Name   string
	Fields []Field
}

// CreateTable creates a table and returns the table corresponding table struct
func CreateTable(ctx context.Context, name string, fields []Field) (*Table, error) {
	path := fmt.Sprintf("./%s-db", name)

	file, err := getFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not open db file: %w", err)
	}

	table := Table{
		Name:   name,
		Fields: fields,
	}

	// write the table struct to the file to act as a header and schema
	file.Write()

}
