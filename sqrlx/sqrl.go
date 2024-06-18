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

// Connection is Queryer + Begin
type Connection interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

// Transactor is implemented by Wrapper
type Transactor interface {
	Transact(context.Context, *TxOptions, Callback) error
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

// Commander runs database queries
type Commander interface {
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
}

type Transaction interface {
	Commander
	TxExtras
}

// TxExtras groups methods which can only be run inside of a transaction
type TxExtras interface {
	Reset(context.Context) error
	PrepareRaw(context.Context, string) (*sql.Stmt, error)
}

type PlaceholderFormat interface {
	ReplacePlaceholders(string) (string, error)
}

type Sqlizer interface {
	ToSql() (string, []interface{}, error)
}

type Wrapper struct {
	db                Connection
	placeholderFormat PlaceholderFormat

	// Max number of retries in acquiring transactions, or retrying due to
	// transient or transaction conflict errors.
	RetryCount int

	// Called when a transaction callback returns an error, if true, will retry
	// the callback when ShouldRetryTransaction is also true.
	// Note this does not effect errors on the Begin() and Commit() calls.
	ShouldRetryTransaction func(error) bool

	DefaultTxOptions *TxOptions

	QueryLogger QueryLogger
}

type QueryLogger interface {
	LogQuery(context.Context, string, ...interface{})
}

type WrapperCommander struct {
	*Wrapper
	Commander
}

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

type CallbackLogger func(context.Context, string)

func (cb CallbackLogger) LogQuery(ctx context.Context, statement string, params ...interface{}) {
	cb(ctx, fmt.Sprintf("QUERY %s", statement))
	for i, param := range params {
		switch param := param.(type) {
		case []byte:
			if len(param) > 1 && param[0] == '{' && param[len(param)-1] == '}' {
				cb(ctx, fmt.Sprintf("  $%d %s", i, string(param)))
				continue
			}
		}
		cb(ctx, fmt.Sprintf("  $%d %#v", i, param))
	}
}

func TestQueryLogger(t interface {
	Log(...interface{})
	Helper()
}) QueryLogger {
	return CallbackLogger(func(ctx context.Context, statement string) {
		t.Helper()
		t.Log(statement)
	})
}

func New(conn Connection, placeholder PlaceholderFormat) (*Wrapper, error) {
	return &Wrapper{
		db:                     conn,
		placeholderFormat:      placeholder,
		RetryCount:             5,
		ShouldRetryTransaction: defaultShouldRetry,
		DefaultTxOptions: &TxOptions{
			ReadOnly:  false,
			Isolation: sql.LevelSerializable,
		},
	}, nil
}

func NewPostgres(conn Connection) *Wrapper {
	return &Wrapper{
		db:                     conn,
		placeholderFormat:      Dollar,
		RetryCount:             5,
		ShouldRetryTransaction: defaultShouldRetry,
		DefaultTxOptions: &TxOptions{
			ReadOnly:  false,
			Isolation: sql.LevelSerializable,
		},
	}
}

func NewWithCommander(conn Connection, placeholder PlaceholderFormat) (*WrapperCommander, error) {
	ww := &Wrapper{
		db:                     conn,
		placeholderFormat:      placeholder,
		RetryCount:             5,
		ShouldRetryTransaction: defaultShouldRetry,
		DefaultTxOptions: &TxOptions{
			ReadOnly:  false,
			Isolation: sql.LevelSerializable,
		},
	}
	commander := &commandWrapper{
		rawCommander: rawDirect{db: conn, PlaceholderFormat: placeholder},
	}

	return &WrapperCommander{
		Wrapper:   ww,
		Commander: commander,
	}, nil
}

type TxOptions struct {
	Isolation sql.IsolationLevel
	ReadOnly  bool

	// Transaction callback will be called more than once to retry some errors.
	// Errors which will result in retries are any error on the transaction
	// Commit() call, or any errors returned from the callback for which
	// `wrapper.ShouldRetryTransaction` returns true
	//
	// Errors from the Begin() call will always retry up to `wrapper.RetryCount`
	Retryable bool
}

type rawCommander interface {
	QueryRaw(context.Context, string, ...interface{}) (*Rows, error)
	ExecRaw(context.Context, string, ...interface{}) (sql.Result, error)
	SelectRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error)
	PlaceholderFormat
}

type Callback func(context.Context, Transaction) error

// Transact calls cb within a transaction. The begin call is retried if
// required. If cb returns an error, the transaction is rolled back, otherwise
// it is committed. Failed commits are not retried, and will return an error
func (w Wrapper) Transact(ctx context.Context, opts *TxOptions, cb Callback) (returnErr error) {

	if opts == nil {
		opts = w.DefaultTxOptions
	}

	var exitWithError error

	for tries := 0; tries < w.RetryCount; tries++ {

		txWrapped := &txWrapper{
			opts:              opts,
			connWrapper:       w,
			PlaceholderFormat: w.placeholderFormat,
			RetryCount:        w.RetryCount,
			queryLogger:       w.QueryLogger,
		}

		commander := &commandWrapper{
			rawCommander: txWrapped,
		}

		if err := txWrapped.begin(ctx); err != nil {
			exitWithError = err
			continue
		}

		if err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("Panic: %s", r)
					fmt.Println("Recovering TX Panic " + err.Error() + "\n" + string(debug.Stack()))
				}
			}()
			return cb(ctx, Tx{
				Commander: commander,
				TxExtras:  txWrapped,
			})
		}(); err != nil {
			if err := txWrapped.tx.Rollback(); err != nil {
				// Retry will be a mess
				return fmt.Errorf("rolling back transaction: %w", err)
			}

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

type Tx struct {
	Commander
	TxExtras
}

type txWrapper struct {
	tx          *sql.Tx
	opts        *TxOptions
	connWrapper Wrapper
	PlaceholderFormat
	RetryCount    int
	isTransaction bool
	queryLogger   QueryLogger
}

func (w *txWrapper) Reset(ctx context.Context) error {
	if err := w.tx.Rollback(); err != nil {
		return err
	}
	return w.begin(ctx)
}

func (w *txWrapper) begin(ctx context.Context) error {
	tx, err := w.connWrapper.db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly:  w.opts.ReadOnly,
		Isolation: w.opts.Isolation,
	})
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	w.tx = tx
	// rollback or commit happen after the callback returns in the initial Transact call
	return nil
}

func (w txWrapper) PrepareRaw(ctx context.Context, str string) (*sql.Stmt, error) {
	return w.tx.PrepareContext(ctx, str)
}

// SelectRaw runs a string + params query, with automatic retry on transient
// errors. Do not use SELECT queries to modify data.
func (w txWrapper) SelectRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error) {
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

// QueryRaw runs a query directly with the driver, returning wrapped rows. It
// will not attempt to retry. No retries are attempted, Use SelectRaw for automatic retries
func (w txWrapper) QueryRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error) {
	if w.queryLogger != nil {
		w.queryLogger.LogQuery(ctx, statement, params...)
	}

	rows, err := w.tx.QueryContext(ctx, statement, params...) // nolint rowserrcheck
	if err != nil {
		return nil, err
	}

	return &Rows{
		IRows: rows,
	}, nil
}

// ExecRaw runs an exec statement directly with the driver. No retries are attempted.
func (w txWrapper) ExecRaw(ctx context.Context, statement string, params ...interface{}) (sql.Result, error) {
	if w.queryLogger != nil {
		w.queryLogger.LogQuery(ctx, statement, params...)
	}

	res, err := w.tx.ExecContext(ctx, statement, params...)
	if err != nil {
		return nil, &QueryError{
			cause:     err,
			Statement: statement,
		}
	}
	return res, nil
}

type rawDirect struct {
	db Connection
	PlaceholderFormat
}

// SelectRaw runs a string + params query
func (w rawDirect) SelectRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error) {
	return w.QueryRaw(ctx, statement, params...)
}

// QueryRaw runs a query directly with the driver, returning wrapped rows. It
// will not attempt to retry. No retries are attempted, Use SelectRaw for automatic retries
func (w rawDirect) QueryRaw(ctx context.Context, statement string, params ...interface{}) (*Rows, error) {
	rows, err := w.db.QueryContext(ctx, statement, params...) // nolint rowserrcheck
	if err != nil {
		return nil, err
	}

	return &Rows{
		IRows: rows,
	}, nil
}

// ExecRaw runs an exec statement directly with the driver. No retries are attempted.
func (w rawDirect) ExecRaw(ctx context.Context, statement string, params ...interface{}) (sql.Result, error) {
	res, err := w.db.ExecContext(ctx, statement, params...)
	if err != nil {
		return nil, &QueryError{
			cause:     err,
			Statement: statement,
		}
	}
	return res, nil
}

// commandWrapper extends a rawCommander with SQ funcs and single row returns.
type commandWrapper struct {
	rawCommander
}

func (w commandWrapper) Exec(ctx context.Context, bb Sqlizer) (sql.Result, error) {
	statement, params, err := bb.ToSql()
	if err != nil {
		return nil, err
	}
	statement, err = w.rawCommander.ReplacePlaceholders(statement)
	if err != nil {
		return nil, err
	}
	return w.rawCommander.ExecRaw(ctx, statement, params...)
}

// Deprecated: Use Exec
func (w commandWrapper) Insert(ctx context.Context, bb Sqlizer) (sql.Result, error) {
	return w.Exec(ctx, bb)
}

// InsertRow is like Exec, but calls result RowsEffected, returning true if
// it is 1, false of 0, or error if > 1
func (w commandWrapper) InsertRow(ctx context.Context, bb Sqlizer) (bool, error) {
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

func (w commandWrapper) InsertStruct(ctx context.Context, tableName string, vals ...interface{}) (sql.Result, error) {
	bb, err := InsertStruct(tableName, vals...)
	if err != nil {
		return nil, err
	}
	return w.Exec(ctx, bb)
}

// Deprecated: Use Exec()
func (w commandWrapper) Update(ctx context.Context, bb Sqlizer) (sql.Result, error) {
	return w.Exec(ctx, bb)
}

// Deprecated: Use Exec()
func (w commandWrapper) Delete(ctx context.Context, bb Sqlizer) (sql.Result, error) {
	return w.Exec(ctx, bb)
}

// Select runs a builder to query, returning Rows. Transient errors will be retried. Do not modify data in a select.
func (w commandWrapper) Select(ctx context.Context, bb Sqlizer) (*Rows, error) {
	statement, params, err := bb.ToSql()
	if err != nil {
		return nil, err
	}

	statement, err = w.rawCommander.ReplacePlaceholders(statement)
	if err != nil {
		return nil, err
	}

	return w.rawCommander.SelectRaw(ctx, statement, params...)

}

// SelectRow returns a single row, otherwise is the same as Select
func (w commandWrapper) SelectRow(ctx context.Context, bb Sqlizer) *Row {
	return rowFromRes(w.Select(ctx, bb))
}

// Query runs the statement once, returning any error, it does not retry and so
// is safe to use for UPDATE RETURNING
func (w commandWrapper) Query(ctx context.Context, bb Sqlizer) (*Rows, error) {
	statement, params, err := bb.ToSql()
	if err != nil {
		return nil, err
	}

	statement, err = w.rawCommander.ReplacePlaceholders(statement)
	if err != nil {
		return nil, err
	}

	rows, err := w.rawCommander.QueryRaw(ctx, statement, params...)
	return rows, err
}

// QueryRow returns a single row, otherwise is the same as Query. No retries are attempted.
func (w commandWrapper) QueryRow(ctx context.Context, bb Sqlizer) *Row {
	return rowFromRes(w.Query(ctx, bb))
}

// QueryRowRaw returns a single row, otherwise is the same as QueryRaw. No
// Retries are attempted, use SelectRowRaw for automatic retries
func (w commandWrapper) QueryRowRaw(ctx context.Context, statement string, params ...interface{}) *Row {
	return rowFromRes(w.rawCommander.QueryRaw(ctx, statement, params...))
}
