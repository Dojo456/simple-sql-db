package engine

import (
	"regexp"
	"strings"
)

func isEmptyString(s string) bool {
	val, _ := regexp.Match(`\S`, []byte(s))

	return !val
}

// cleanString removes all newline characters and replaces it with spaces.
// It also removes redundant white space characters.
func cleanString(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "  ", " ")

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
