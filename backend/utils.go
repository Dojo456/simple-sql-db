package backend

import "os"

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
