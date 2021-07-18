package sqrlx

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/lib/pq"
)

type testPlaceholder struct{}

func (testPlaceholder) ReplacePlaceholders(sql string) (string, error) {
	// This is useless in the real world, should be enough to make tests
	return strings.ReplaceAll(sql, "?", "!"), nil
}

type testSqlizer struct {
	str  string
	args []interface{}
	err  error
}

func (ts testSqlizer) ToSql() (string, []interface{}, error) {
	return ts.str, ts.args, ts.err
}

type testError string

func (te testError) Error() string {
	return string(te)
}

func testTransaction(t *testing.T) (*QueryWrapper, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err.Error())
	}
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err.Error())
	}

	txWrapped := &QueryWrapper{
		tx: tx,
		//opts: opts,
		//connWrapper:       w,
		placeholderFormat: testPlaceholder{},
		RetryCount:        1,
	}

	return txWrapped, mock
}

func TestQueryHappy(t *testing.T) {
	ctx := context.Background()
	tx, mock := testTransaction(t)

	mock.ExpectQuery("SELECT a FROM b WHERE c = !").
		WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("A"))

	q := testSqlizer{
		str:  "SELECT a FROM b WHERE c = ?",
		args: []interface{}{"hello"},
		err:  nil,
	}
	_, err := tx.Query(ctx, q)
	if err != nil {
		t.Fatalf("Got error %s", err.Error())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err.Error())
	}

}

func TestQueryError(t *testing.T) {
	ctx := context.Background()
	tx, _ := testTransaction(t)

	q := testSqlizer{
		err: testError("TEST"),
	}
	_, err := tx.Query(ctx, q)
	if err == nil {
		t.Fatal("Expected Error")
	}
	if !errors.Is(err, q.err) {
		t.Fatalf("Returned Error '%s' did not wrap statement error", err)
	}
}

func TestQueryRowHappy(t *testing.T) {
	ctx := context.Background()
	tx, mock := testTransaction(t)

	mock.ExpectQuery("SELECT a FROM b WHERE c = !").
		WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("A"))

	q := testSqlizer{
		str:  "SELECT a FROM b WHERE c = ?",
		args: []interface{}{"hello"},
		err:  nil,
	}

	row := tx.QueryRow(ctx, q)
	if row.err != nil {
		t.Fatalf("Got error %s", row.err.Error())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestQueryRowStatementError(t *testing.T) {
	ctx := context.Background()
	tx, _ := testTransaction(t)

	q := testSqlizer{
		err: testError("TEST"),
	}
	row := tx.QueryRow(ctx, q)
	err := row.Scan(nil)
	if err == nil {
		t.Errorf("Expected Passthrough Error")
	}
	if !errors.Is(err, q.err) {
		t.Fatalf("Returned Error '%s' did not wrap statement error", err)
	}
}

func TestSelectRetry(t *testing.T) {

	ctx := context.Background()
	tx, mock := testTransaction(t)
	tx.RetryCount = 4

	var err1 = testError("1")
	var err2 = testError("2")

	mock.ExpectQuery("SELECT a FROM b WHERE c = !").
		WillReturnError(err1)

	mock.ExpectQuery("SELECT a FROM b WHERE c = !").
		WillReturnError(err2)

	mock.ExpectQuery("SELECT a FROM b WHERE c = !").
		WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("A"))

	q := testSqlizer{
		str:  "SELECT a FROM b WHERE c = ?",
		args: []interface{}{"hello"},
		err:  nil,
	}

	_, err := tx.Select(ctx, q)
	if err != nil {
		t.Fatalf("Got error %s", err.Error())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestQueryRowServerError(t *testing.T) {
	mockRows := &MockRows{
		NextVal: true,
		ScanImpl: func(vals ...interface{}) error {
			if len(vals) != 1 {
				t.Fatalf("Should have 1 vals, got %v", vals)
			}
			if bv, ok := vals[0].(*string); !ok {
				t.Fatalf("Should be a *string")
			} else if *bv != "str" {
				t.Fatalf("First val should be the field, was %v", *bv)
			}
			return nil
		},
	}
	r := Row{
		Rows: mockRows,
	}
	str := "str"
	if err := r.Scan(&str); err != nil {
		t.Fatal(err.Error())
	}

	if !mockRows.DidClose {
		t.Errorf("Rows did not get closed")
	}

}

type MockResult struct {
	lastInsertId int64
	rowsAffected int64
}

func (m MockResult) LastInsertId() (int64, error) {
	return m.lastInsertId, nil
}
func (m MockResult) RowsAffected() (int64, error) {
	return m.rowsAffected, nil
}

func TestExecHappy(t *testing.T) {

	ctx := context.Background()
	tx, mock := testTransaction(t)

	q := testSqlizer{
		str:  "INSERT INTO b VALUES (?)",
		args: []interface{}{"c"},
		err:  nil,
	}

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO b VALUES (!)")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := tx.Exec(ctx, q)
	if err != nil {
		t.Fatalf("Got error %s", err.Error())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestInsertRowChanged(t *testing.T) {

	for _, tc := range []struct {
		count  int64
		expect bool
		err    bool
	}{
		{count: 0, expect: false},
		{count: 1, expect: true},
		{count: 2, expect: false, err: true},
	} {
		t.Run(fmt.Sprintf("%d", tc.count), func(t *testing.T) {
			ctx := context.Background()
			tx, mock := testTransaction(t)

			q := testSqlizer{
				str:  "INSERT INTO b VALUES (?)",
				args: []interface{}{"c"},
				err:  nil,
			}

			mock.ExpectExec(regexp.QuoteMeta("INSERT INTO b VALUES (!)")).
				WillReturnResult(sqlmock.NewResult(1, tc.count))

			didInsert, err := tx.InsertRow(ctx, q)
			if tc.err {
				if err == nil {
					t.Fatal("No Error")
				}
				return
			} else {
				if err != nil {
					t.Fatalf("Got error %s", err.Error())
				}
			}

			if didInsert != tc.expect {
				t.Errorf("Expected false")
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err.Error())
			}
		})
	}
}

func TestExecStatementError(t *testing.T) {
	ctx := context.Background()
	tx, _ := testTransaction(t)

	q := testSqlizer{
		err: testError("TEST"),
	}
	_, err := tx.Exec(ctx, q)
	if err == nil {
		t.Errorf("Expected Passthrough Error")
	}
	if !errors.Is(err, q.err) {
		t.Fatalf("Returned Error '%s' did not wrap statement error", err)
	}
}

func TestExecServerError(t *testing.T) {
	ctx := context.Background()
	tx, mock := testTransaction(t)

	q := testSqlizer{
		str:  "INSERT INTO b VALUES (?)",
		args: []interface{}{"c"},
		err:  nil,
	}

	throwErr := testError("ERR")
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO b VALUES (!)")).
		WillReturnError(throwErr)

	_, err := tx.Exec(ctx, q)
	if err == nil {
		t.Errorf("Expected Passthrough Error")
	}
	if !errors.Is(err, throwErr) {
		t.Fatalf("Returned Error '%s' did not wrap statement error", err)
	}
}

func TestTxPanic(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err.Error())
	}

	mock.ExpectBegin()
	mock.ExpectRollback()

	w, err := New(db, testPlaceholder{})
	if err != nil {
		t.Fatal(err.Error())
	}

	ctx := context.Background()

	err = w.Transact(ctx, nil, func(ctx context.Context, tx Transaction) error {
		panic("Test Panic")
	})
	if err == nil {
		t.Errorf("Expected an Error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err.Error())
	}
}
