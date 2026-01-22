package pipe_test

import (
	"fmt"
	"reflect"
	"testing"

	pipe "github.com/saylorsolutions/pipelib"
	"github.com/stretchr/testify/assert"
)

var _ pipe.RowReader = (*TestRowReader)(nil)

type TestRowReader struct {
	readerErr error
	curRow    int
	rows      [][]string
}

func NewTestRowReader(rows [][]string) *TestRowReader {
	return &TestRowReader{
		curRow: -1,
		rows:   rows,
	}
}

func (r *TestRowReader) Next() bool {
	r.curRow++
	return r.curRow < len(r.rows)
}

func (r *TestRowReader) Err() error {
	return r.readerErr
}

func (r *TestRowReader) SetErr(err error) {
	r.readerErr = err
}

func (r *TestRowReader) Scan(targets ...any) error {
	if r.curRow < 0 {
		return fmt.Errorf("must call Next to begin iteration")
	}
	if r.curRow >= len(r.rows) {
		r.readerErr = fmt.Errorf("no further rows to scan")
		return r.readerErr
	}
	row := r.rows[r.curRow]
	if len(targets) != len(row) {
		return fmt.Errorf("the number of targets (%d) should match the number of columns (%d)", len(targets), len(row))
	}
	for i := 0; i < len(row); i++ {
		if !setString(targets[i], row[i]) {
			return fmt.Errorf("failed to scan target value %d in row %d", i, r.curRow)
		}
	}
	return nil
}

func setString(ref any, val string) (rval bool) {
	reflect.ValueOf(ref).Elem().SetString(val)
	return true
}

func TestProduceRows(t *testing.T) {
	rows := [][]string{
		{"a", "b", "c"},
		{"d", "e", "f"},
	}
	reader := NewTestRowReader(rows)
	var numRead int
	ctx := pipe.NewContext()
	producer, produced := pipe.Produce(ctx, pipe.ProduceRows(reader, func() ([]any, func() (string, error)) {
		var (
			a, b, c string
		)
		return []any{&a, &b, &c}, func() (string, error) {
			return fmt.Sprintf("%s, %s, %s", a, b, c), nil
		}
	}))
	consumer := pipe.Consume(ctx, produced, func(ctx *pipe.Context, in string) error {
		t.Logf("Consumed string '%s' in iteration %d", in, numRead)
		switch numRead {
		case 0:
			assert.Equal(t, "a, b, c", in)
		case 1:
			assert.Equal(t, "d, e, f", in)
		default:
			t.Error("Unexpected additional calls to consume")
		}
		numRead++
		return nil
	})
	pipe.RunPipeline(producer, consumer)
	assert.Equal(t, 2, numRead)
}
