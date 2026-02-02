package pipe

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
)

var _ RowReader = (*CsvRowReader)(nil)

type CsvRowReader struct {
	err        error
	reader     *csv.Reader
	numColumns int
	skipHeader bool
	curOffset  int
	cols       []string
}

// CsvOption is an option that can be passed to OpenCsv to customize how the source is read.
type CsvOption func(r *CsvRowReader) error

// CsvSkipHeader will instruct CsvRowReader to skip the first row in the source.
func CsvSkipHeader() CsvOption {
	return func(r *CsvRowReader) error {
		r.skipHeader = true
		return nil
	}
}

// CsvColumnSeparator allows customizing the column separator character.
// Defaults to a comma.
func CsvColumnSeparator(sep rune) CsvOption {
	return func(r *CsvRowReader) error {
		r.reader.Comma = sep
		return nil
	}
}

// CsvTrimLeadingSpace will remove leading whitespace in a field, even if the separator character is a space.
func CsvTrimLeadingSpace() CsvOption {
	return func(r *CsvRowReader) error {
		r.reader.TrimLeadingSpace = true
		return nil
	}
}

// CsvFieldsPerRecord specifies the number of expected fields in a row.
// An error will be returned if this is not the case.
func CsvFieldsPerRecord(fields int) CsvOption {
	return func(r *CsvRowReader) error {
		if fields <= 0 {
			return fmt.Errorf("invalid field width %d, must be > 0", fields)
		}
		r.reader.FieldsPerRecord = fields
		return nil
	}
}

// OpenCsv creates a new CsvRowReader that reads the given source, and accepts options for customization.
func OpenCsv(source io.Reader, opts ...CsvOption) (*CsvRowReader, error) {
	reader := csv.NewReader(source)
	crr := &CsvRowReader{
		reader:     reader,
		numColumns: -1,
		curOffset:  -1,
	}
	for _, opt := range opts {
		if err := opt(crr); err != nil {
			return nil, err
		}
	}
	return crr, nil
}

func (c *CsvRowReader) Next() bool {
	if c.err != nil {
		return false
	}
	c.curOffset++
	cols, err := c.reader.Read()
	if err != nil {
		c.cols = nil
		c.err = err
		return false
	}
	c.cols = cols
	if c.numColumns == -1 {
		c.numColumns = len(c.cols)
	} else if len(c.cols) != c.numColumns {
		c.err = fmt.Errorf("expected %d columns per row, but got %d", c.numColumns, len(c.cols))
		return false
	}
	if c.curOffset == 0 && c.skipHeader {
		return c.Next()
	}
	return true
}

func (c *CsvRowReader) Err() error {
	return c.err
}

func (c *CsvRowReader) Scan(targets ...any) error {
	if c.err != nil {
		return c.err
	}
	if c.curOffset < 0 {
		return errors.New("must call Next before any call to Scan")
	}
	if len(targets) != len(c.cols) {
		c.err = fmt.Errorf("expected %d targets, but received %d", len(c.cols), len(targets))
	}
	for i := range targets {
		str, ok := targets[i].(*string)
		if !ok {
			c.err = fmt.Errorf("scan argument %d is not a pointer to string", i)
			return c.err
		}
		if str == nil {
			c.err = fmt.Errorf("scan argument %d is a nil pointer", i)
			return c.err
		}
		*str = c.cols[i]
	}
	return nil
}
