package pipe_test

import (
	"io"
	"strings"
	"testing"

	pipe "github.com/saylorsolutions/pipelib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenCsv(t *testing.T) {
	data := `A,B,C
D,E,F
`
	reader, err := pipe.OpenCsv(strings.NewReader(data))
	require.NoError(t, err)

	var a, b, c string
	assert.True(t, reader.Next())
	assert.Nil(t, reader.Err())
	assert.NoError(t, reader.Scan(&a, &b, &c))
	assert.Equal(t, "A", a)
	assert.Equal(t, "B", b)
	assert.Equal(t, "C", c)
	assert.True(t, reader.Next())
	assert.Nil(t, reader.Err())
	assert.NoError(t, reader.Scan(&a, &b, &c))
	assert.Equal(t, "D", a)
	assert.Equal(t, "E", b)
	assert.Equal(t, "F", c)
	assert.False(t, reader.Next())
	assert.ErrorIs(t, reader.Err(), io.EOF)
}

func TestCsvSkipHeader(t *testing.T) {
	data := "A,B,C\n"
	r := strings.NewReader(data)
	reader, err := pipe.OpenCsv(r, pipe.CsvSkipHeader())
	require.NoError(t, err)
	assert.False(t, reader.Next(), "Skipping the header for a single line source should return no rows")
	assert.ErrorIs(t, reader.Err(), io.EOF)
}

func TestCsvFieldsPerRecord(t *testing.T) {
	data := "A,B,C\n"
	t.Run("Expected columns", func(t *testing.T) {
		reader, err := pipe.OpenCsv(strings.NewReader(data),
			pipe.CsvFieldsPerRecord(3))
		require.NoError(t, err)
		var a, b, c string
		assert.True(t, reader.Next())
		assert.NoError(t, reader.Scan(&a, &b, &c))
		assert.Equal(t, "A", a)
		assert.Equal(t, "B", b)
		assert.Equal(t, "C", c)
	})
	t.Run("Unexpected column count", func(t *testing.T) {
		reader, err := pipe.OpenCsv(strings.NewReader(data),
			pipe.CsvFieldsPerRecord(4))
		require.NoError(t, err)
		assert.False(t, reader.Next())
		assert.NotNil(t, reader.Err())
	})
}

func TestCsvTrimLeadingSpace(t *testing.T) {
	data := " A,\tB,    C\n"
	reader, err := pipe.OpenCsv(strings.NewReader(data),
		pipe.CsvTrimLeadingSpace())
	require.NoError(t, err)
	var a, b, c string
	assert.True(t, reader.Next())
	assert.NoError(t, reader.Scan(&a, &b, &c))
	assert.Equal(t, "A", a)
	assert.Equal(t, "B", b)
	assert.Equal(t, "C", c)
}

func TestCsvColumnSeparator(t *testing.T) {
	data := "A|B|C\n"
	reader, err := pipe.OpenCsv(strings.NewReader(data),
		pipe.CsvColumnSeparator('|'))
	require.NoError(t, err)
	var a, b, c string
	assert.True(t, reader.Next())
	assert.NoError(t, reader.Scan(&a, &b, &c))
	assert.Equal(t, "A", a)
	assert.Equal(t, "B", b)
	assert.Equal(t, "C", c)
}
