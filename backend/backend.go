// Package backend The backend package interacts with storage devices to provide the data storage functionality of the
// database.
package backend

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
	return anyToB(v.Val)
}

// Field is essentially a column in a table.
type Field struct {
	Name string
	Type Primitive
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

	return Field{}, fieldNotExistErr(fieldName, t.GetName())
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
