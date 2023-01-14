package backend

import "context"

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
	UpdateRows(ctx context.Context, values []Value, filter *Filter) (int, error)
}
