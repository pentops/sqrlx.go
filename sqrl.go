package sqrlx

import (
	"context"
	"database/sql"
	"reflect"

	sq "github.com/elgris/sqrl"
)

type Connection interface {
	Queryer
}

type Queryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

// ColumnType is implemented by *sql.ColumnType
type ColumnType interface {
	DatabaseTypeName() string
	DecimalSize() (precision, scale int64, ok bool)
	Length() (length int64, ok bool)
	Name() string
	Nullable() (nullable, ok bool)
	ScanType() reflect.Type
}

type Wrapper struct {
	db                Connection
	placeholderFormat sq.PlaceholderFormat
}

func New(conn Connection, placeholder sq.PlaceholderFormat) (*Wrapper, error) {
	return &Wrapper{
		db:                conn,
		placeholderFormat: placeholder,
	}, nil
}

func (w Wrapper) QueryRow(ctx context.Context, bb *sq.SelectBuilder) *Row {
	statement, params, err := bb.PlaceholderFormat(w.placeholderFormat).ToSql()
	if err != nil {
		return &Row{
			err: err,
		}
	}

	rows, err := w.db.QueryContext(ctx, statement, params...)
	if err != nil {
		return &Row{
			err: err,
		}
	}

	return &Row{
		Rows: rows,
	}
}

func (w Wrapper) Insert(ctx context.Context, bb *sq.InsertBuilder) (sql.Result, error) {
	statement, params, err := bb.PlaceholderFormat(w.placeholderFormat).ToSql()
	if err != nil {
		return nil, err
	}

	return w.db.ExecContext(ctx, statement, params...)
}

func (w Wrapper) InsertStruct(ctx context.Context, tableName string, vals interface{}) (sql.Result, error) {
	bb, err := InsertStruct(tableName, vals)
	if err != nil {
		return nil, err
	}
	return w.Insert(ctx, bb)
}

func (w Wrapper) Update(ctx context.Context, bb *sq.UpdateBuilder) (sql.Result, error) {
	statement, params, err := bb.PlaceholderFormat(w.placeholderFormat).ToSql()
	if err != nil {
		return nil, err
	}

	return w.db.ExecContext(ctx, statement, params...)
}

func (w Wrapper) Query(ctx context.Context, bb *sq.SelectBuilder) (*Rows, error) {
	statement, params, err := bb.PlaceholderFormat(w.placeholderFormat).ToSql()

	if err != nil {
		return nil, err
	}

	return w.QueryRaw(ctx, statement, params...)

}

func (w Wrapper) QueryRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error) {
	rows, err := w.db.QueryContext(ctx, statement, params...)
	if err != nil {
		return nil, err
	}

	return &Rows{
		IRows: rows,
	}, nil
}
