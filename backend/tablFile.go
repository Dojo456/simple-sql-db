package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// CreateTable creates a table and returns the table corresponding table struct.
func CreateTable(ctx context.Context, name string, fields []Field) (OperableTable, error) {
	path := getTableFilePath(name)

	file, err := createFile(path)
	if err != nil {
		if errors.Is(err, fileAlreadyExistsError) {
			return nil, fmt.Errorf(`table with name "%s" already exists`, name)
		} else {
			return nil, fmt.Errorf("could not create table db file: %w", err)
		}
	}

	var lock sync.RWMutex

	table := table{
		mrw:          &lock,
		rowCount:     0,
		file:         file,
		Name:         name,
		Fields:       fields,
		rowByteCount: calculateRowSize(fields),
	}

	fmt.Println("creating table: ")
	fmt.Println(table)

	// write the table struct to the file to act as a table and schema
	err = table.writeTableHeader()
	if err != nil {
		return nil, fmt.Errorf("could not write table header: %w", err)
	}

	return &table, nil
}

func getTableFilePath(name string) string {
	return fmt.Sprintf("./database/%s-db", name)
}

// writeTableHeader writes the header to initialize a table file. It will flush a table if there is already data.
func (t *table) writeTableHeader() error {
	t.mrw.Lock()
	defer t.mrw.Unlock()

	data, err := json.Marshal(*t)
	if err != nil {
		return fmt.Errorf("could not encode table metadata: %w", err)
	}

	headerByteCount := len(data) + 8

	// begin header with an unsigned i64 that is the number of bytes of the table size, including the number itself
	header := i64ToB(int64(headerByteCount))
	header = append(header, data...)

	// the header also contains an uint64 slice of capacity 200 that acts as a buffer to minimize array shift operations
	// when a row is deleted, the row's index is added to this array and on insertion, rows should be inserted into
	// the next value in this array. If the buffer is exceeded, then a whole table shift operation must be performed
	freeRows := make([]byte, 200*64)
	header = append(header, freeRows...)

	_, err = t.file.Write(header)
	if err != nil {
		return err
	}

	t.headerByteCount = int64(headerByteCount)
	t.fileByteCount = int64(headerByteCount)

	return nil
}

/*
Below functions are used to read a table file and perform operators on an already
existing table
*/

// OpenTable returns a table struct that tableReader and tableWriters can attach to. If table with given name does not
// exist, an error will be returned. An opened table needs to be cleaned up later through the Cleanable interface
func OpenTable(name string) (OperableTable, error) {
	path := getTableFilePath(name)

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			f.Close()
			return nil, fmt.Errorf("table with name %s does not exist", name)
		} else {
			f.Close()
			return nil, fmt.Errorf("could not open table file: %w", err)
		}
	}

	table, err := readTableFile(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("could not read table file: %w", err)
	}

	var lock sync.RWMutex
	table.mrw = &lock

	return table, nil
}

// readTableFile reads a tableFile's header to create a table struct that can then be used for operations.
func readTableFile(file *os.File) (*table, error) {
	headerSizeBytes := make([]byte, 8)

	_, err := file.Read(headerSizeBytes)
	if err != nil {
		return nil, err
	}

	headerSize := bToI64(headerSizeBytes)
	header := make([]byte, headerSize-8)

	_, err = file.Read(header)
	if err != nil {
		return nil, err
	}

	var table table
	err = json.Unmarshal(header, &table)
	if err != nil {
		return nil, err
	}

	table.headerByteCount = headerSize
	table.file = file
	table.rowByteCount = calculateRowSize(table.Fields)

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("could not open file stats: %w", err)
	}

	table.fileByteCount = stat.Size()
	table.rowCount = (table.fileByteCount - table.headerByteCount) / table.rowByteCount

	return &table, nil
}

// calculateRowSize calculates the numbers of bytes each row of the table takes. This should be called on table
// initialization and stored into the table struct.
func calculateRowSize(fields []Field) int64 {
	var sum int64

	for _, field := range fields {
		sum += field.Type.Size()
	}

	return sum
}
