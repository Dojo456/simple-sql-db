package language

import "github.com/Dojo456/simple-sql-db/backend"

// Command is an executable SQL Command that requires 0 or more arguments afterwards
type Command int

const (
	CreateTableCommand Command = iota
	SelectCommand
	InsertCommand
	DeleteCommand
	UpdateCommand
)

func getCommand(keywords []keyword) (*Command, error) {
	if len(keywords) == 0 {
		return nil, nil
	}

	var returner Command
	found := false

	first := keywords[0]
	switch first {
	case KeywordCreate:
		{
			if len(keywords) > 1 {
				second := keywords[1]
				switch second {
				case KeywordTable:
					returner = CreateTableCommand
					found = true
				}
			}
		}
	case KeywordSelect:
		returner = SelectCommand
		found = true
	case KeywordInsert:
		{
			if len(keywords) > 1 {
				second := keywords[1]
				switch second {
				case KeywordInto:
					returner = InsertCommand
					found = true
				}
			}
		}
	case KeywordDelete:
		{
			if len(keywords) > 1 {
				second := keywords[1]
				switch second {
				case KeywordFrom:
					returner = DeleteCommand
					found = true
				}
			}
		}
	case KeywordUpdate:
		returner = UpdateCommand
		found = true
	}

	if !found {
		return nil, nil
	}

	return &returner, nil
}

// UntypedValue is an unparsed value that has the potential to be parsed into a backend.Value.
type UntypedValue struct {
	Val       string
	FieldName string
}

type CreateTableArgs struct {
	TableName string
	Fields    []backend.Field
}

type SelectArgs struct {
	TableName  string
	FieldNames []string
	AllFields  bool
	Filter     *WhereClause
}

type InsertArgs struct {
	TableName     string
	Values        []UntypedValue
	HasFieldNames bool
}

type DeleteArgs struct {
	TableName string
	Filter    *WhereClause
}

type UpdateArgs struct {
	TableName string
	Values    []UntypedValue
	Filter    *WhereClause
}
