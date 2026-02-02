package pipe

import (
	"io"
)

// RowReader allows for easier testing and more generic tabular data source interactions.
//
// A Close method is not included, because callers are responsible for ensuring that resources are cleaned up after use.
type RowReader interface {
	// Next should be called before any call to Scan, including the first one.
	// Next should return whether reading the next row was successful.
	// When false is returned, Err should be used to determine why the call to Next wasn't successful.
	Next() bool
	// Err returns the last error that occurred during a call to Next or Scan.
	// Errors encountered during these calls can stop iteration through the source.
	Err() error
	// Scan will scan data into the given targets, which should be compatible, non-nil pointers.
	// Scan should report if Next has not yet been called, which should be recoverable by a call to Next.
	// Any other error is expected to halt progression through the source.
	Scan(targets ...any) error
}

// RowMapper is a function that produces scan targets and a function that returns the mapped column values as the value type.
//
// The idea is that for each invocation of the RowMapper, a new value type T is created and pointers to fields are returned.
// The producer function (second return value) allows for adding validation or normalization logic to the value type after scanning has completed successfully.
// In the simplest case, the value type is returned by this function as-is.
//
// If an error is returned by the RowMapper producer function, then it will be returned from ProduceRows.
type RowMapper[T any] func() (scanTargets []any, producer func() (T, error))

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
