package backend

import (
	"encoding/binary"
	"errors"
	"log"
	"math"
	"os"
	"strings"
)

func compareValues[T comparable](v1 T, operator Operator, v2 T) bool {
	switch operator {
	case OperatorEqual:
		return v1 == v2
	case OperatorNotEqual:
		return v1 != v2
	}

	return false
}

// exclusive returns the elements that are in s1 but not in s2
func exclusive[T comparable](s1 []T, s2 []T) []T {
	var returner []T

	for _, e := range s1 {
		if !contains(s2, e) {
			returner = append(returner, e)
		}
	}

	return returner
}

func contains[T comparable](slice []T, element T) bool {
	for _, t := range slice {
		if t == element {
			return true
		}
	}

	return false
}

var fileAlreadyExistsError error = errors.New("file already exists")

// createFile creates a file at the given path. It will throw an error if the file already exists
func createFile(path string) (*os.File, error) {
	// check if database directory exists
	dirPath := "database"
	if _, err := os.Stat(dirPath); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(dirPath, os.ModePerm)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}

	// check if file exists
	exists := true

	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			exists = false
		} else {
			return nil, err
		}
	}

	// create file if not exists
	if exists {
		return nil, fileAlreadyExistsError
	} else {
		file, err := os.Create(path)
		if err != nil {
			return nil, err
		}

		return file, nil
	}
}

// sToB converts a string to a byte slice of size 1024
func sToB(val string) []byte {
	returner := make([]byte, 0, 1024)

	returner = append(returner, []byte(val)...)

	return returner[:1024]
}

// bToS converts a byte slice of size 1024 to a string
func bToS(val []byte) string {
	var builder strings.Builder

	for _, b := range val {
		if b == 0 {
			break
		}

		builder.WriteByte(b)
	}

	return builder.String()
}

// i64ToB converts an int64 to a byte slice of size 8
func i64ToB(val int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(val))

	return b
}

// bToI64 converts a byte slice of size 8 to an int64
func bToI64(val []byte) int64 {
	return int64(binary.LittleEndian.Uint64(val))
}

// f64ToB converts a float64 to a byte slice of size 8
func f64ToB(val float64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(val))

	return b
}

// bToF64 converts a byte slice of size 8 to a float64
func bToF64(val []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(val))
}
