package sqrlx

import (
	"context"
	"database/sql"
	"reflect"

	sq "github.com/elgris/sqrl"
)

// QueryError is thrown by all exec and query commands to wrap the driver error.
// It includes the statement causing the error
type QueryError struct {
	cause     error
	Statement string
}

// Cause gives the driver error which was thrown
func (err QueryError) Cause() error {
	return err.cause
}

// Error is the cause error + the statement causing it
func (err QueryError) Error() string {
	return err.cause.Error() + " `" + err.Statement + "` "
}

// Queryer runs database queries
type Queryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

// Connection is Queryer + Begin
type Connection interface {
	Queryer
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
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
	RetryCount        int
}

var _ Transaction = Wrapper{}

func New(conn Connection, placeholder sq.PlaceholderFormat) (*Wrapper, error) {
	return &Wrapper{
		db: conn,
		QueryWrapper: QueryWrapper{
			db:                conn,
			placeholderFormat: placeholder,
			RetryCount:        3,
		},
	}, nil
}

// Transact calls cb within a transaction. The begin call is retried if
// required. If cb returns an error, the transaction is rolled back, otherwise
// it is committed. Failed commits are not retried, and will return an error
func (w Wrapper) Transact(ctx context.Context, opts *sql.TxOptions, cb func(context.Context, Transaction) error) error {
	var tx *sql.Tx
	var err error
	for tries := 0; tries < w.RetryCount; tries++ {
		tx, err = w.db.BeginTx(ctx, opts)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}

	txWrapped := &QueryWrapper{
		db:                tx,
		placeholderFormat: w.placeholderFormat,
		RetryCount:        w.RetryCount,
	}

	if err := cb(ctx, txWrapped); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (w QueryWrapper) Insert(ctx context.Context, bb *sq.InsertBuilder) (sql.Result, error) {
	statement, params, err := bb.PlaceholderFormat(w.placeholderFormat).ToSql()
	if err != nil {
		return nil, err
	}

	return w.ExecRaw(ctx, statement, params...)
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

// Select runs a builder to query, returning Rows. Transient errors will be retried. Do not modify data in a select.
func (w QueryWrapper) Select(ctx context.Context, bb *sq.SelectBuilder) (*Rows, error) {
	statement, params, err := bb.PlaceholderFormat(w.placeholderFormat).ToSql()

	if err != nil {
		return nil, err
	}

	var rows *Rows
	for tries := 0; tries < w.RetryCount; tries++ {
		rows, err = w.QueryRaw(ctx, statement, params...)
		if err == nil || err == sql.ErrNoRows {
			return rows, err
		}
	}

	return rows, err
}

// SelectRow returns a single row, otherwise is the same as Select
func (w QueryWrapper) SelectRow(ctx context.Context, bb *sq.SelectBuilder) *Row {
	rows, err := w.Select(ctx, bb)
	if err != nil {
		return &Row{
			err: err,
		}
	}

	return &Row{
		Rows: rows,
	}
}

// SelectRaw runs a string + params query, with automatic retry on transient errors
func (w QueryWrapper) SelectRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error) {
	var rows *Rows
	var err error
	for tries := 0; tries < w.RetryCount; tries++ {
		rows, err = w.QueryRaw(ctx, statement, params...)
		if err == nil {
			break
		}
	}
	return rows, err
}

// QueryRaw runs a query directly with the driver, returning wrapped rows. It
// will not attempt to retry. Use SelectRaw for automatic retries
func (w QueryWrapper) QueryRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error) {
	rows, err := w.db.QueryContext(ctx, statement, params...)
	if err != nil {
		return nil, err
	}

	return &Rows{
		IRows: rows,
	}, nil
}

// ExecRaw runs an exec statement directly with the driver. No retries are attempted.
func (w QueryWrapper) ExecRaw(ctx context.Context, statement string, params ...interface{}) (sql.Result, error) {
	res, err := w.db.ExecContext(ctx, statement, params...)
	if err != nil {
		return nil, &QueryError{
			cause:     err,
			Statement: statement,
		}
	}
	return res, nil
}
