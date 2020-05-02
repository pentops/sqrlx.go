package sqrlx

import (
	"context"
	"database/sql"
	"testing"

	sq "github.com/elgris/sqrl"
)

type MockConn struct {
	Connection

	queryContext func(context.Context, string, ...interface{}) (*sql.Rows, error)
	execContext  func(context.Context, string, ...interface{}) (sql.Result, error)
}

func (mc MockConn) QueryContext(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return mc.queryContext(ctx, sql, args)
}

func (mc MockConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return mc.execContext(ctx, sql, args)
}

type mockRows struct{}

func TestQuery(t *testing.T) {

	mockConn := &MockConn{}

	w, err := New(mockConn, sq.Dollar)
	if err != nil {
		t.Fatal(err.Error())
	}

	ctx := context.Background()

	t.Run("Happy", func(t *testing.T) {
		mockConn.queryContext = func(ctx context.Context, statement string, args ...interface{}) (*sql.Rows, error) {
			if statement != "SELECT a FROM b WHERE c = $1" {
				t.Errorf("Statement: %s", statement)
			}
			return &sql.Rows{}, nil
		}
		q := sq.Select("a").From("b").Where("c = ?", "hello")
		_, err := w.Query(ctx, q)
		if err != nil {
			t.Fatalf("Got error %s", err.Error())
		}
	})

	t.Run("Squrl Error", func(t *testing.T) {
		q := sq.Select()
		_, err := w.Query(ctx, q)
		if err == nil {
			t.Errorf("Expected Error")
		}
	})
}

func TestQueryRow(t *testing.T) {

	mockConn := &MockConn{}

	w, err := New(mockConn, sq.Dollar)
	if err != nil {
		t.Fatal(err.Error())
	}

	ctx := context.Background()

	t.Run("Happy", func(t *testing.T) {
		mockConn.queryContext = func(ctx context.Context, statement string, args ...interface{}) (*sql.Rows, error) {
			if statement != "SELECT a FROM b WHERE c = $1" {
				t.Errorf("Statement: %s", statement)
			}
			return &sql.Rows{}, nil
		}
		q := sq.Select("a").From("b").Where("c = ?", "hello")
		row := w.QueryRow(ctx, q)
		if row.err != nil {
			t.Fatalf("Got error %s", err.Error())
		}
		// Can't actually test row here because of the sql interface.
	})

	t.Run("Squrl Error", func(t *testing.T) {
		q := sq.Select()
		row := w.QueryRow(ctx, q)
		if row.err == nil {
			t.Errorf("Expected Error")
		}

		if err := row.Scan(nil); err == nil {
			t.Errorf("Expected Passthrough Error")
		}
	})

	t.Run("Row itself", func(t *testing.T) {

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

	})
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

func TestInsert(t *testing.T) {

	mockConn := &MockConn{}

	w, err := New(mockConn, sq.Dollar)
	if err != nil {
		t.Fatal(err.Error())
	}

	ctx := context.Background()

	t.Run("Happy", func(t *testing.T) {
		mockConn.execContext = func(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
			t.Log(statement)
			if statement != "INSERT INTO b VALUES ($1)" {
				t.Errorf("Statement: %s", statement)
			}
			return MockResult{}, nil
		}
		q := sq.Insert("b").Values("c")
		_, err := w.Insert(ctx, q)
		if err != nil {
			t.Fatalf("Got error %s", err.Error())
		}

	})

	t.Run("Squrl Error", func(t *testing.T) {
		q := sq.Insert("b")
		_, err := w.Insert(ctx, q)
		if err == nil {
			t.Errorf("Expected Error")
		}
	})
}

func TestUpdate(t *testing.T) {

	mockConn := &MockConn{}

	w, err := New(mockConn, sq.Dollar)
	if err != nil {
		t.Fatal(err.Error())
	}

	ctx := context.Background()

	t.Run("Happy", func(t *testing.T) {
		mockConn.execContext = func(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
			t.Log(statement)
			if statement != "UPDATE b SET c = $1" {
				t.Errorf("Statement: %s", statement)
			}
			return MockResult{}, nil
		}
		q := sq.Update("b").Set("c", "world")
		_, err := w.Update(ctx, q)
		if err != nil {
			t.Fatalf("Got error %s", err.Error())
		}

	})

	t.Run("Squrl Error", func(t *testing.T) {
		q := sq.Update("b")
		_, err := w.Update(ctx, q)
		if err == nil {
			t.Errorf("Expected Error")
		}
	})
}
