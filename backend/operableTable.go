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
	GetRows(ctx context.Context, fields []string, filters []Filter) ([]Row, error)
	DeleteRows(ctx context.Context, filters []Filter) (int, error)
	UpdateRows(ctx context.Context, values []Value, filters []Filter) (int, error)
}
