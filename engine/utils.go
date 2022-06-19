package engine

import "regexp"

func isEmptyString(s string) bool {
	val, _ := regexp.Match(`\S`, []byte(s))

	return !val
}
