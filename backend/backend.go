// The backend package interacts with storage devices to provide the data storage functionality of the
// database.
package backend

import "fmt"

// Put puts the value at the specified key
func Put(key string, value interface{}) error {
	data[key] = value

	return nil
}

// Get gets the value at the specified key
func Get(key string) (interface{}, error) {
	val, exists := data[key]

	if !exists {
		return nil, fmt.Errorf("key does not exist")
	}

	return val, nil
}

var data map[string]interface{}

func init() {
	data = map[string]interface{}{}
}
