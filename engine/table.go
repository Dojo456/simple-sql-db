package engine

import "context"

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

// CreateTable creates a table and returns the table id
func CreateTable(ctx context.Context, name string, fields []Field) (string, error) {

}
