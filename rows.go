package pipe

import (
	"io"
)

// RowReader allows for easier testing and more generic tabular data source interactions.
//
// A Close method is not included, because callers are responsible for ensuring that resources are cleaned up after use.
type RowReader interface {
	Next() bool
	Err() error
	Scan(targets ...any) error
}

// RowMapper is a function that produces scan targets and a function that returns the mapped column values as the value type.
//
// The idea is that for each invocation of the RowMapper, a new value type T is created and pointers to fields are returned.
// The producer function (second return value) allows for adding validation or normalization logic to the value type after scanning has completed successfully.
// In the simplest case, the value type is returned by this function as-is.
//
// If an error is returned by the RowMapper producer function, then it will be returned from ProduceRows.
type RowMapper[T any] func() ([]any, func() (T, error))

// ProduceRows creates a ProducerHandler that reads from the given RowReader, and uses the RowMapper to get the mapped value type.
func ProduceRows[Out any](rows RowReader, rowMapper RowMapper[Out]) ProducerHandler[Out] {
	if rows == nil {
		panic("nil rows")
	}
	var mt Out
	return func(ctx *Context) (Out, error) {
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				ctx.Error("error preparing next row to scan", "error", err)
				return mt, err
			}
			return mt, io.EOF
		}
		scanTargets, producer := rowMapper()
		if err := rows.Scan(scanTargets...); err != nil {
			ctx.Error("failed to scan row", "error", err)
			return mt, err
		}
		val, err := producer()
		if err != nil {
			ctx.Error("error mapping row values to value type", "error", err)
			return mt, err
		}

		return val, nil
	}
}
