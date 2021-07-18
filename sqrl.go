package sqrlx

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"runtime/debug"
)

// QueryError is thrown by all exec and query commands to wrap the driver error.
// It includes the statement causing the error
type QueryError struct {
	cause     error
	Statement string
}

// Cause gives the driver error which was thrown
func (err QueryError) Unwrap() error {
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
	ExecRaw(context.Context, string, ...interface{}) (sql.Result, error)
	Exec(context.Context, Sqlizer) (sql.Result, error)

	QueryRaw(context.Context, string, ...interface{}) (*Rows, error)
	Query(context.Context, Sqlizer) (*Rows, error)

	QueryRowRaw(context.Context, string, ...interface{}) *Row
	QueryRow(context.Context, Sqlizer) *Row

	SelectRow(context.Context, Sqlizer) *Row
	Select(context.Context, Sqlizer) (*Rows, error)
	Insert(context.Context, Sqlizer) (sql.Result, error)
	InsertRow(context.Context, Sqlizer) (bool, error)
	InsertStruct(context.Context, string, ...interface{}) (sql.Result, error)
	Update(context.Context, Sqlizer) (sql.Result, error)
	Delete(context.Context, Sqlizer) (sql.Result, error)

	Reset(context.Context) error
}

type PlaceholderFormat interface {
	ReplacePlaceholders(string) (string, error)
}

type Sqlizer interface {
	ToSql() (string, []interface{}, error)
}

type Wrapper struct {
	db Connection
	//QueryWrapper
	placeholderFormat      PlaceholderFormat
	RetryCount             int
	ShouldRetryTransaction func(error) bool
}

type QueryWrapper struct {
	tx                *sql.Tx
	opts              *sql.TxOptions
	connWrapper       Wrapper
	placeholderFormat PlaceholderFormat
	RetryCount        int
	isTransaction     bool
}

//var _ Transaction = Wrapper{}

func defaultShouldRetry(err error) bool {
	var sqlState = ""

	// github.com/lib/pq
	if getPGCodeErr, ok := err.(interface {
		Get(byte) string
	}); ok {
		sqlState = getPGCodeErr.Get('C')
	}

	// TODO: Other drivers. Really this should be part of the database/sql library.

	if sqlState == "40001" {
		// serilaization failure, in the SQL standard
		return true
	}
	return false
}

func New(conn Connection, placeholder PlaceholderFormat) (*Wrapper, error) {
	return &Wrapper{
		db:                     conn,
		placeholderFormat:      placeholder,
		RetryCount:             5,
		ShouldRetryTransaction: defaultShouldRetry,
	}, nil
}

var DefaultTxOptions = &sql.TxOptions{
	ReadOnly:  false,
	Isolation: sql.LevelSerializable,
}

// Transact calls cb within a transaction. The begin call is retried if
// required. If cb returns an error, the transaction is rolled back, otherwise
// it is committed. Failed commits are not retried, and will return an error
func (w Wrapper) Transact(ctx context.Context, opts *sql.TxOptions, cb func(context.Context, Transaction) error) (returnErr error) {

	if opts == nil {
		opts = DefaultTxOptions
	}

	var exitWithError error

	for tries := 0; tries < w.RetryCount; tries++ {
		tx, err := w.db.BeginTx(ctx, opts)
		if err != nil {
			exitWithError = err
			continue
		}

		txWrapped := &QueryWrapper{
			tx:                tx,
			opts:              opts,
			connWrapper:       w,
			placeholderFormat: w.placeholderFormat,
			RetryCount:        w.RetryCount,
		}

		if err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("Panic: %s", r)
					fmt.Println("Recovering TX Panic " + err.Error() + "\n" + string(debug.Stack()))
				}
			}()
			return cb(ctx, txWrapped)
		}(); err != nil {
			txWrapped.tx.Rollback()
			if w.ShouldRetryTransaction != nil {
				if w.ShouldRetryTransaction(err) {
					exitWithError = err
					continue
				}
			}
			return err
		}

		if err := txWrapped.tx.Commit(); err != nil {
			exitWithError = fmt.Errorf("committing transaction: (%d/%d) %w", tries+1, w.RetryCount, err)
			continue
		}
		return nil
	}
	return exitWithError
}

func (w *QueryWrapper) Reset(ctx context.Context) error {
	if err := w.tx.Rollback(); err != nil {
		return err
	}
	newTx, err := w.connWrapper.db.BeginTx(ctx, w.opts)
	if err != nil {
		return err
	}
	w.tx = newTx
	// rollback or commit happen after the callback returns in the initial Transact call
	return nil
}

func (w QueryWrapper) Exec(ctx context.Context, bb Sqlizer) (sql.Result, error) {
	statement, params, err := bb.ToSql()
	if err != nil {
		return nil, err
	}
	statement, err = w.placeholderFormat.ReplacePlaceholders(statement)
	if err != nil {
		return nil, err
	}
	return w.ExecRaw(ctx, statement, params...)
}

// Deprecated: Use Exec
func (w QueryWrapper) Insert(ctx context.Context, bb Sqlizer) (sql.Result, error) {
	return w.Exec(ctx, bb)
}

// InsertRow is like Exec, but calls result RowsEffected, returning true if
// it is 1, false of 0, or error if > 1
func (w QueryWrapper) InsertRow(ctx context.Context, bb Sqlizer) (bool, error) {
	res, err := w.Exec(ctx, bb)
	if err != nil {
		return false, err
	}

	count, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	if count == 0 {
		return false, nil
	}
	if count == 1 {
		return true, nil
	}
	return false, fmt.Errorf("%d rows effected by InsertRow", count)
}

func (w QueryWrapper) InsertStruct(ctx context.Context, tableName string, vals ...interface{}) (sql.Result, error) {
	bb, err := InsertStruct(tableName, vals...)
	if err != nil {
		return nil, err
	}
	return w.Exec(ctx, bb)
}

// Deprecated: Use Exec()
func (w QueryWrapper) Update(ctx context.Context, bb Sqlizer) (sql.Result, error) {
	return w.Exec(ctx, bb)
}

// Deprecated: Use Exec()
func (w QueryWrapper) Delete(ctx context.Context, bb Sqlizer) (sql.Result, error) {
	return w.Exec(ctx, bb)
}

// Select runs a builder to query, returning Rows. Transient errors will be retried. Do not modify data in a select.
func (w QueryWrapper) Select(ctx context.Context, bb Sqlizer) (*Rows, error) {
	statement, params, err := bb.ToSql()
	if err != nil {
		return nil, err
	}

	statement, err = w.placeholderFormat.ReplacePlaceholders(statement)
	if err != nil {
		return nil, err
	}

	return w.SelectRaw(ctx, statement, params...)

}

// SelectRow returns a single row, otherwise is the same as Select
func (w QueryWrapper) SelectRow(ctx context.Context, bb Sqlizer) *Row {
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

// SelectRaw runs a string + params query, with automatic retry on transient
// errors. Do not use SELECT queries to modify data.
func (w QueryWrapper) SelectRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error) {
	var err error
	var rows *Rows
	var firstError error
	for tries := 0; tries < w.RetryCount; tries++ {
		rows, err = w.QueryRaw(ctx, statement, params...)
		if err == nil || err == sql.ErrNoRows || w.isTransaction {
			return rows, err
		}

		// TODO: Return immediately if it isn't a connection issue
		if firstError == nil {
			firstError = err
		}
	}

	if firstError != nil {
		return nil, firstError
	}
	return rows, nil
}

// Query runs the statement once, returning any error, it does not retry and so
// is safe to use for UPDATE RETURNING
func (w QueryWrapper) Query(ctx context.Context, bb Sqlizer) (*Rows, error) {
	statement, params, err := bb.ToSql()
	if err != nil {
		return nil, err
	}

	statement, err = w.placeholderFormat.ReplacePlaceholders(statement)
	if err != nil {
		return nil, err
	}

	rows, err := w.QueryRaw(ctx, statement, params...)
	return rows, err
}

// QueryRow returns a single row, otherwise is the same as Query, it will not retry
func (w QueryWrapper) QueryRow(ctx context.Context, bb Sqlizer) *Row {
	rows, err := w.Query(ctx, bb)
	if err != nil {
		return &Row{
			err: err,
		}
	}

	return &Row{
		Rows: rows,
	}
}

// QueryRaw runs a query directly with the driver, returning wrapped rows. It
// will not attempt to retry. Use SelectRaw for automatic retries
func (w QueryWrapper) QueryRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error) {
	rows, err := w.tx.QueryContext(ctx, statement, params...)
	if err != nil {
		return nil, err
	}

	return &Rows{
		IRows: rows,
	}, nil
}

// QueryRowRaw returns a single row, otherwise is the same as QueryRaw
func (w QueryWrapper) QueryRowRaw(ctx context.Context, statement string, params ...interface{}) *Row {
	rows, err := w.tx.QueryContext(ctx, statement, params...)
	if err != nil {
		return &Row{
			err: err,
		}
	}

	return &Row{
		Rows: rows,
	}
}

// ExecRaw runs an exec statement directly with the driver. No retries are attempted.
func (w QueryWrapper) ExecRaw(ctx context.Context, statement string, params ...interface{}) (sql.Result, error) {
	res, err := w.tx.ExecContext(ctx, statement, params...)
	if err != nil {
		return nil, &QueryError{
			cause:     err,
			Statement: statement,
		}
	}
	return res, nil
}
