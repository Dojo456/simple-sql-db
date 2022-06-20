package backend

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
)

var fileAlreadyExistsError error = errors.New("file already exists")

// createFile creates a file at the given path. It will throw an error if the file already exists
func createFile(path string) (*os.File, error) {
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

// getFile opens and returns the file at the given path. If the file does not exist, it will be created
func getFile(path string) (*os.File, error) {
	// check if file exists
	file, err := os.Open(path)
	if err == nil && file != nil {
		return file, err
	}

	// create file if not exists
	if os.IsNotExist(err) {
		file, err = os.Create(path)
		if err != nil {
			return nil, err
		}

		return file, nil
	} else {
		return nil, err
	}
}

// i64tob converts an uint64 to a byte slice of size 8
func i64tob(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)

	return b
}

// btoi64 converts a byte slice of size 8 to an uint64
func btoi64(val []byte) uint64 {
	return binary.LittleEndian.Uint64(val)
}

// f64tob converts a float64 to a byte slice of size 4
func f64tob(val float64) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint64(b, math.Float64bits(val))

	return b
}

// btof64 converts a byte slice of size 4 to an uint64
func btof64(val []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(val))
}
