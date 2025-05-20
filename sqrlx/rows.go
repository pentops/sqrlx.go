package sqrlx

import (
	"database/sql"
	"fmt"
)

// IRows is the interface of *sql.Rows
type IRows interface {
	Scan(...any) error
	Columns() ([]string, error)
	Next() bool
	Close() error
	Err() error
}

var _ IRows = &sql.Rows{}

type Rows struct {
	IRows
}

// Each iterates over the rows, handling close and error checking.
func (rr *Rows) Each(fn func(Scannable) error) error {
	var err error
	for rr.Next() {
		err = fn(rr)
		if err != nil {
			return err
		}
	}

	if err = rr.Err(); err != nil {
		_ = rr.Close()
		return err
	}

	if err = rr.Close(); err != nil {
		return err
	}

	return nil
}

type Row struct {
	Rows IRows
	err  error
}

func rowFromRes(rows *Rows, err error) *Row {
	if err != nil {
		return &Row{
			err: err,
		}
	}

	return &Row{
		Rows: rows,
	}
}

func (r Row) Scan(into ...any) error {
	// partial clone of sql.Row.Scan, but skipping the safety for the RawBytes issue
	if r.err != nil {
		return fmt.Errorf("existing row error in scan: %w", r.err)
	}

	defer r.Rows.Close()
	if !r.Rows.Next() {
		if err := r.Rows.Err(); err != nil {
			return fmt.Errorf("error in row scan row next: %w", err)
		}

		return sql.ErrNoRows
	}
	if err := r.Rows.Scan(into...); err != nil {
		return fmt.Errorf("error in row scan: %w", err)
	}
	return r.Rows.Close()
}

func (r Row) ScanStruct(into any) error {
	if err := ScanStruct(r, into); err != nil {
		return fmt.Errorf("scan struct: %w", err)
	}
	return nil
}

func (r Row) Columns() ([]string, error) {
	if r.err != nil {
		return nil, fmt.Errorf("existing row error in columns: %w", r.err)
	}
	return r.Rows.Columns()
}
