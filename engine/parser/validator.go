package parser

import "fmt"

func Validate(statement string) error {
	tokens, err := split(statement)
	if err != nil {
		return err
	}

	for _, token := range tokens {
		fmt.Printf("%+v\n", token)
	}

	return nil
}
