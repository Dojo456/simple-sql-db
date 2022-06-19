package backend

import (
	"encoding/binary"
	"errors"
	"os"
)

var fileAlreadyExistsError error = errors.New("file already exists")

// createFile creates a file at the given path. It will throw an error if the file already exists
func createFile(path string) (*os.File, error) {
	// check if file exists
	_, err := os.Stat(path)
	if err == nil {
		return nil, err
	}

	// create file if not exists
	if os.IsNotExist(err) {
		file, err := os.Create(path)
		if err != nil {
			return nil, err
		}

		return file, nil
	} else {
		return nil, fileAlreadyExistsError
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

// i32tob converts an uint32 to a byte slice of size 4
func i32tob(val uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, val)

	return b
}

// btoi32 converts a byte slice of size 4 to an uint32
func btoi32(val []byte) uint32 {
	return binary.LittleEndian.Uint32(val)
}
