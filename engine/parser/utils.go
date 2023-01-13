package parser

import (
	"fmt"
	"regexp"
	"strings"
)

func parseQuotedString(s string) string {
	return s[1 : len(s)-2]
}

func isEmptyString(s string) bool {
	val, _ := regexp.Match(`\S`, []byte(s))

	return !val
}

func assertFloat64(val interface{}) (float64, error) {
	f, ok := val.(float64)

	if !ok {
		return 0, fmt.Errorf("not float64")
	}

	return f, nil
}

func assertInt64(val interface{}) (int64, error) {
	i, ok := val.(int64)
	if !ok {
		return 0, fmt.Errorf("not int64")
	}

	return i, nil
}

func assertString(val interface{}) (string, error) {
	s, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("not string")
	}

	return s, nil
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

// Equal tells whether a and b contain the same elements.
// A nil argument is equivalent to an empty slice.
func Equal[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
