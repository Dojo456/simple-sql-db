package language

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Dojo456/simple-sql-db/backend"
)

func escapeQuotedString(s string) string {
	if s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}

	return s
}

func isEmptyString(s string) bool {
	val, _ := regexp.Match(`\S`, []byte(s))

	return !val
}

var regexpSpaceBeforeEqual = regexp.MustCompile(" =")
var regexpSpaceAfterEqual = regexp.MustCompile("= ")
var regexpSpaceBeforeComma = regexp.MustCompile(" ,")
var regexpSpaceAfterComma = regexp.MustCompile(", ")

// cleanString removes all newline characters and replaces it with spaces. It also removes redundant white
// space characters, such as the ones before and after an equal sign.
func cleanString(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "  ", " ")

	s = regexpSpaceBeforeEqual.ReplaceAllString(s, "=")
	s = regexpSpaceAfterEqual.ReplaceAllString(s, "=")

	s = regexpSpaceBeforeComma.ReplaceAllString(s, ",")
	s = regexpSpaceAfterComma.ReplaceAllString(s, ",")

	return s
}

// parseField takes in a string that is the name and data type of the field separated by a space
//
// i.e. "name string"
func parseField(s string) (backend.Field, error) {
	tokens := strings.Split(s, " ")
	if len(tokens) != 2 {
		return backend.Field{}, fmt.Errorf("%s is not acceptable", s)
	}

	name := tokens[0]
	dataType := backend.Primitive(strings.ToLower(tokens[1]))

	if !dataType.IsValid() {
		return backend.Field{}, fmt.Errorf("%s is not a valid data type", dataType)
	}

	return backend.Field{
		Name: name,
		Type: dataType,
	}, nil
}

func stripTableNameFromField(fieldName string, tableName string) string {
	tokens := strings.Split(fieldName, ".")

	if len(tokens) == 2 && tokens[0] == tableName { // field name is {tableName}.{fieldName}
		return tokens[1]
	}

	return fieldName
}

// NewValueForField creates a Value for the Field. This is the preferred way to create a Value struct. If the val is
// of the correct Go type for that field, it will be entered directly. If it is of string type and the field is not,
// it will attempt to parse the value into the correct type.
func NewValueForField(field backend.Field, val interface{}) (backend.Value, error) {
	switch field.Type {
	case backend.PrimitiveString:
		{
			s, ok := val.(string)
			if !ok {
				return backend.Value{}, fmt.Errorf("must be a string")
			}

			return backend.Value{
				Type:      backend.PrimitiveString,
				Val:       escapeQuotedString(s),
				FieldName: field.Name,
			}, nil
		}

	case backend.PrimitiveInt:
		{
			i, ok := val.(int64)
			if !ok {
				s, ok := val.(string)
				if !ok {
					return backend.Value{}, fmt.Errorf("could not parse int")
				}

				sI, err := strconv.Atoi(s)
				if err != nil {
					return backend.Value{}, fmt.Errorf("could not parse int")
				}

				i = int64(sI)
			}
			return backend.Value{
				Type:      backend.PrimitiveInt,
				Val:       i,
				FieldName: field.Name,
			}, nil
		}
	case backend.PrimitiveFloat:
		{
			f, ok := val.(float64)
			if !ok {
				s, ok := val.(string)
				if !ok {
					return backend.Value{}, fmt.Errorf("could not parse float")
				}

				sF, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return backend.Value{}, fmt.Errorf("could not parse float")
				}

				f = sF
			}
			return backend.Value{
				Type:      backend.PrimitiveFloat,
				Val:       f,
				FieldName: field.Name,
			}, nil
		}
	}

	return backend.Value{}, nil
}
