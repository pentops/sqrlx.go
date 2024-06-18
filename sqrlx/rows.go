package sqrlx

import (
	"database/sql"
	"fmt"
)

// IRows is the interface of *sql.Rows
type IRows interface {
	Scan(...interface{}) error
	Columns() ([]string, error)
	Next() bool
	Close() error
	Err() error
}

var _ IRows = &sql.Rows{}

type Rows struct {
	IRows
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

func (r Row) Scan(into ...interface{}) error {
	// partial clone of sql.Row.Scan, but skipping the safety for the RawBytes issue
	if r.err != nil {
		return r.err
	}

	defer r.Rows.Close()
	if !r.Rows.Next() {
		if err := r.Rows.Err(); err != nil {
			return err
		}

		return sql.ErrNoRows
	}
	if err := r.Rows.Scan(into...); err != nil {
		return err
	}
	return r.Rows.Close()
}

func (r Row) ScanStruct(into interface{}) error {
	if err := ScanStruct(r, into); err != nil {
		return fmt.Errorf("scan struct: %w", err)
	}
	return nil
}

func (r Row) Columns() ([]string, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.Rows.Columns()
}
