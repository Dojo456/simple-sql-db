package engine

import (
	"context"
)

type SQLEngine struct {
}

// New returns a new engine instance that can then be used to execute SQL statements
func New(ctx context.Context) (*SQLEngine, error) {
	return &SQLEngine{}, nil
}
