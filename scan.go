package sqrlx

import (
	"fmt"
	"reflect"
)

type Scannable interface {
	Scan(...interface{}) error
	Columns() ([]string, error)
}

func addNamed(structCols map[string]interface{}, rv reflect.Value, override bool) error {

	// TODO: Check types to raise errors
	rt := rv.Type()
	for i := 0; i < rv.NumField(); i++ {

		field := rt.Field(i)

		tag := field.Tag
		tagName := tag.Get("sql")
		if tagName == "-" {
			continue
		}
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			if err := addNamed(structCols, rv.Field(i), false); err != nil {
				return err
			}
			continue
		}

		if tagName == "" {
			continue
		}

		fieldInterface := rv.Field(i).Addr().Interface()
		if override {
			structCols[tagName] = fieldInterface
		} else if _, ok := structCols[tagName]; !ok {
			structCols[tagName] = fieldInterface
		}
	}
	return nil
}

func StructColNames(dest interface{}, prefix string) ([]string, error) {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("ScanStruct requires a pointer to a struct")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("ScanStruct requires a pointer to a struct")
	}

	structCols := map[string]interface{}{}

	if err := addNamed(structCols, rv, true); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(structCols))
	for name := range structCols {
		names = append(names, prefix+name)
	}
	return names, nil
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

	if err := addNamed(structCols, rv, true); err != nil {
		return err
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
