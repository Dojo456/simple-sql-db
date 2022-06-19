package backend

import (
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

// i32tob converts an uint32 to a byte slice of size 4
func i32tob(val uint32) []byte {
	r := make([]byte, 4)
	for i := uint32(0); i < 4; i++ {
		r[i] = byte((val >> (8 * i)) & 0xff)
	}
	return r
}

// btoi32 converts a byte slice of size 4 to an uint32
func btoi32(val []byte) uint32 {
	r := uint32(0)
	for i := uint32(0); i < 4; i++ {
		r |= uint32(val[i]) << (8 * i)
	}
	return r
}
