package language

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Dojo456/simple-sql-db/backend"
)

func parseQuotedString(s string) string {
	return s[1 : len(s)-2]
}

func isEmptyString(s string) bool {
	val, _ := regexp.Match(`\S`, []byte(s))

	return !val
}

var regexpSpaceBeforeEqual = regexp.MustCompile(" =")
var regexpSpaceAfterEqual = regexp.MustCompile("= ")

// cleanString removes all newline characters and replaces it with spaces. It also removes redundant white
// space characters, such as the ones before and after an equal sign.
func cleanString(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "  ", " ")
	s = strings.ReplaceAll(s, ", ", ",")

	s = regexpSpaceBeforeEqual.ReplaceAllString(s, "=")
	s = regexpSpaceAfterEqual.ReplaceAllString(s, "=")

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
