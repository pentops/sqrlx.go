package sqrlx

import (
	"testing"
)

type MockRows struct {
	ColumnsVal []string
	ScanImpl   func(...any) error
	ErrVal     error
	NextVal    bool

	DidClose bool
}

func (ms *MockRows) Scan(vals ...any) error {
	return ms.ScanImpl(vals...)
}

func (ms *MockRows) Columns() ([]string, error) {
	return ms.ColumnsVal, nil
}

func (ms *MockRows) Close() error {
	ms.DidClose = true
	return nil
}

func (ms *MockRows) Err() error {
	return ms.ErrVal
}

func (ms *MockRows) Next() bool {
	return ms.NextVal
}

func TestScanErrors(t *testing.T) {

	ms := &MockRows{
		ColumnsVal: []string{},
	}

	if err := ScanStruct(ms, nil); err == nil {
		t.Errorf("Should be bad type error")
	}

	if err := ScanStruct(ms, "string"); err == nil {
		t.Errorf("should be bad type error")
	}

	str := ""
	if err := ScanStruct(ms, &str); err == nil {
		t.Errorf("should be bad type error")
	}

	t.Run("Happy Path", func(t *testing.T) {

		ms.ColumnsVal = []string{"b", "a"}

		ms.ScanImpl = func(vals ...any) error {
			if len(vals) != 2 {
				t.Fatalf("Should have 2 vals, got %v", vals)
			}
			if bv, ok := vals[0].(*string); !ok {
				t.Fatalf("Should be a *string")
			} else if *bv != "b-val" {
				t.Fatalf("First val should be the b field, was %v", *bv)
			}
			return nil
		}

		v := struct {
			A string `sql:"a"`
			B string `sql:"b"`
		}{
			A: "a-val", // using the values to test Scan, in reality scan would wipe them
			B: "b-val",
		}
		if err := ScanStruct(ms, &v); err != nil {
			t.Fatal(err.Error())
		}
	})

}
