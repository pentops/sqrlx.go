package sqrlx

import (
	"context"
	"database/sql"
	"reflect"

	sq "github.com/elgris/sqrl"
)

type Connection interface {
	Queryer
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
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

type Transaction interface {
	SelectRow(context.Context, *sq.SelectBuilder) *Row
	Select(context.Context, *sq.SelectBuilder) (*Rows, error)
	Insert(context.Context, *sq.InsertBuilder) (sql.Result, error)
	InsertStruct(context.Context, string, ...interface{}) (sql.Result, error)
	Update(context.Context, *sq.UpdateBuilder) (sql.Result, error)

	QueryRaw(context.Context, string, ...interface{}) (*Rows, error)
	ExecRaw(context.Context, string, ...interface{}) (sql.Result, error)
}

type Wrapper struct {
	db Connection
	QueryWrapper
}

type QueryWrapper struct {
	db                Queryer
	placeholderFormat sq.PlaceholderFormat
}

var _ Transaction = Wrapper{}

func New(conn Connection, placeholder sq.PlaceholderFormat) (*Wrapper, error) {
	return &Wrapper{
		db: conn,
		QueryWrapper: QueryWrapper{
			db:                conn,
			placeholderFormat: placeholder,
		},
	}, nil
}

func (w Wrapper) Transact(ctx context.Context, opts *sql.TxOptions, cb func(context.Context, Transaction) error) error {
	tx, err := w.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	txWrapped := &QueryWrapper{
		db:                tx,
		placeholderFormat: w.placeholderFormat,
	}

	if err := cb(ctx, txWrapped); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (w QueryWrapper) SelectRow(ctx context.Context, bb *sq.SelectBuilder) *Row {
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

func (w QueryWrapper) Insert(ctx context.Context, bb *sq.InsertBuilder) (sql.Result, error) {
	statement, params, err := bb.PlaceholderFormat(w.placeholderFormat).ToSql()
	if err != nil {
		return nil, err
	}

	return w.ExecRaw(ctx, statement, params...)
}

func (w QueryWrapper) ExecRaw(ctx context.Context, statement string, params ...interface{}) (sql.Result, error) {
	res, err := w.db.ExecContext(ctx, statement, params...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (w QueryWrapper) InsertStruct(ctx context.Context, tableName string, vals ...interface{}) (sql.Result, error) {
	bb, err := InsertStruct(tableName, vals...)
	if err != nil {
		return nil, err
	}
	return w.Insert(ctx, bb)
}

func (w QueryWrapper) Update(ctx context.Context, bb *sq.UpdateBuilder) (sql.Result, error) {
	statement, params, err := bb.PlaceholderFormat(w.placeholderFormat).ToSql()
	if err != nil {
		return nil, err
	}

	return w.db.ExecContext(ctx, statement, params...)
}

func (w QueryWrapper) Select(ctx context.Context, bb *sq.SelectBuilder) (*Rows, error) {
	statement, params, err := bb.PlaceholderFormat(w.placeholderFormat).ToSql()

	if err != nil {
		return nil, err
	}

	return w.QueryRaw(ctx, statement, params...)

}

func (w QueryWrapper) QueryRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error) {
	rows, err := w.db.QueryContext(ctx, statement, params...)
	if err != nil {
		return nil, err
	}

	return &Rows{
		IRows: rows,
	}, nil
}
