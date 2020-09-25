package sqrlx

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sq "github.com/elgris/sqrl"
	_ "github.com/lib/pq"
)

func TestQuery(t *testing.T) {

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err.Error())
	}

	w, err := New(db, sq.Dollar)
	if err != nil {
		t.Fatal(err.Error())
	}

	ctx := context.Background()

	t.Run("Happy", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT a FROM b WHERE c = ").
			WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("A"))
		mock.ExpectCommit()
		q := sq.Select("a").From("b").Where("c = ?", "hello")
		_, err := w.Select(ctx, q)
		if err != nil {
			t.Fatalf("Got error %s", err.Error())
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err.Error())
		}

	})

	t.Run("Sq Error", func(t *testing.T) {
		q := sq.Select()
		_, err := w.Select(ctx, q)
		if err == nil {
			t.Errorf("Expected Error")
		}
	})
}

func TestQueryRow(t *testing.T) {

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err.Error())
	}

	w, err := New(db, sq.Dollar)
	if err != nil {
		t.Fatal(err.Error())
	}

	ctx := context.Background()

	t.Run("Happy", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT a FROM b WHERE c = \\$1").
			WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("A"))
		mock.ExpectCommit()

		q := sq.Select("a").From("b").Where("c = ?", "hello")
		row := w.SelectRow(ctx, q)
		if row.err != nil {
			t.Fatalf("Got error %s", err.Error())
		}
		// Can't actually test row here because of the sql interface.
	})

	t.Run("Squrl Error", func(t *testing.T) {
		q := sq.Select()
		row := w.SelectRow(ctx, q)
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

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err.Error())
	}
	w, err := New(db, sq.Dollar)
	if err != nil {
		t.Fatal(err.Error())
	}

	ctx := context.Background()

	t.Run("Happy", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO b VALUES ($1)")).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
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

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err.Error())
	}

	w, err := New(db, sq.Dollar)
	if err != nil {
		t.Fatal(err.Error())
	}

	ctx := context.Background()

	t.Run("Happy", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta("UPDATE b SET c = $1")).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

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
