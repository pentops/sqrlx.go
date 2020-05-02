package sqrlx

import (
	"fmt"
	"reflect"
)

type Scannable interface {
	Scan(...interface{}) error
	Columns() ([]string, error)
}

// ScanStruct scans scannable once, stores vals into the struct.
func ScanStruct(src Scannable, dest interface{}) error {

	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("ScanStruct requires a pointer to a struct")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("ScanStruct requires a pointer to a struct")
	}

	structCols := map[string]interface{}{}

	// TODO: Check types to raise errors
	rt := reflect.TypeOf(dest).Elem()
	for i := 0; i < rv.NumField(); i++ {
		tag := rt.Field(i).Tag
		tagName := tag.Get("sql")
		if tagName == "" || tagName == "-" {
			continue
		}
		fieldInterface := rv.Field(i).Addr().Interface()
		structCols[tagName] = fieldInterface
	}

	cols, err := src.Columns()
	if err != nil {
		return err
	}

	toScan := make([]interface{}, len(cols))

	for idx, name := range cols {
		structCol, ok := structCols[name]
		if !ok {
			return fmt.Errorf("No matching struct field for %s", name)
		}
		toScan[idx] = structCol
	}

	return src.Scan(toScan...)
}
