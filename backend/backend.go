// Package backend The backend package interacts with storage devices to provide the data storage functionality of the
// database.
package backend

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
)

// Primitive represents all the data types that the database can store.
type Primitive string

const (
	PrimitiveString Primitive = "string" // 256 runes max (1024 bytes)
	PrimitiveInt    Primitive = "int"    // int64 (8 bytes)
	PrimitiveFloat  Primitive = "float"  // float64 (8 bytes)
	PrimitiveBool   Primitive = "bool"   // bool (1 byte)
)

func (p Primitive) IsValid() bool {
	switch p {
	case PrimitiveString, PrimitiveInt, PrimitiveFloat, PrimitiveBool:
		return true
	}

	return false
}

func (p Primitive) Size() int64 {
	switch p {
	case PrimitiveString:
		return 1024
	case PrimitiveInt, PrimitiveFloat:
		return 8
	case PrimitiveBool:
		return 1
	}

	return 0
}

// Value is a single cell in a table. It allows type-safe operations between Go primitives and DB primitives (which is
// represented using the Primitive type).
//
// Do not create using struct literal, instead use the NewValue method on the Field type.
type Value struct {
	Type      Primitive
	Val       interface{}
	FieldName string
}

func (v *Value) Bytes() []byte {
	switch val := v.Val.(type) {
	case string:
		return sToB(val)
	case int64:
		return i64ToB(val)
	case float64:
		return f64ToB(val)
	}

	return nil
}

// Field is essentially a column in a table.
type Field struct {
	Name string
	Type Primitive
}

// NewValue creates a Value for the Field. This is the preferred way to create a Value struct. If the val is
// of the correct Go type for that field, it will be entered directly. If it is of string type and the field is not,
// it will attempt to parse the value into the correct type.
func (field Field) NewValue(val interface{}) (*Value, error) {
	switch field.Type {
	case PrimitiveString:
		{
			s, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("must be string")
			}
			return &Value{
				Type:      PrimitiveString,
				Val:       s,
				FieldName: field.Name,
			}, nil
		}

	case PrimitiveInt:
		{
			i, ok := val.(int64)
			if !ok {
				s, ok := val.(string)
				if !ok {
					return nil, fmt.Errorf("could not parse int")
				}

				sI, err := strconv.Atoi(s)
				if err != nil {
					return nil, fmt.Errorf("could not parse int")
				}

				i = int64(sI)
			}
			return &Value{
				Type:      PrimitiveInt,
				Val:       i,
				FieldName: field.Name,
			}, nil
		}
	case PrimitiveFloat:
		{
			f, ok := val.(float64)
			if !ok {
				s, ok := val.(string)
				if !ok {
					return nil, fmt.Errorf("could not parse float")
				}

				sF, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return nil, fmt.Errorf("could not parse float")
				}

				f = sF
			}
			return &Value{
				Type:      PrimitiveFloat,
				Val:       f,
				FieldName: field.Name,
			}, nil
		}
	}

	return nil, nil
}

type table struct {
	mrw             *sync.RWMutex
	file            *os.File
	fileByteCount   int64
	headerByteCount int64
	rowByteCount    int64
	rowCount        int64
	Name            string
	Fields          []Field
}

type OperableTable interface {
	Cleanup() error
	GetName() string
	GetFields() []Field
	FieldWithName(fieldName string) (Field, error)
	HasField(fieldName string) bool
	HasFieldWithType(fieldName string, fieldType Primitive) bool
	InsertRow(ctx context.Context, vals []Value) (int, error)
	GetRows(ctx context.Context, fields []string, filter *Filter) ([]Row, error)
	DeleteRows(ctx context.Context, filter *Filter) (int, error)
}

func (t *table) GetName() string {
	return t.Name
}

func (t *table) GetFields() []Field {
	returner := make([]Field, len(t.Fields))

	copy(returner, t.Fields)

	return returner
}

func (t *table) Cleanup() error {
	return t.file.Close()
}

func (t *table) FieldWithName(fieldName string) (Field, error) {
	for _, field := range t.Fields {
		if field.Name == fieldName {
			return field, nil
		}
	}

	return Field{}, fmt.Errorf("%s.%s does not exist", t.Name, fieldName)
}

func (t *table) HasField(fieldName string) bool {
	for _, field := range t.Fields {
		if field.Name == fieldName {
			return true
		}
	}

	return false
}

func (t *table) HasFieldWithType(fieldName string, fieldType Primitive) bool {
	for _, field := range t.Fields {
		if field.Name == fieldName && field.Type == fieldType {
			return true
		}
	}

	return false
}
