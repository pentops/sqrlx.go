package sqrlx

import (
	"fmt"
	sq "github.com/elgris/sqrl"
	"reflect"
)

func InsertStruct(table string, srcs ...interface{}) (*sq.InsertBuilder, error) {

	builder := sq.Insert(table)

	names := make([]string, 0)

	for idx, src := range srcs {

		rv := reflect.ValueOf(src)
		if rv.Kind() != reflect.Ptr {
			return nil, fmt.Errorf("InsertStruct requires a pointer to a struct")
		}
		rv = rv.Elem()
		if rv.Kind() != reflect.Struct {
			return nil, fmt.Errorf("InsertStruct requires a pointer to a struct")
		}

		structCols := map[string]interface{}{}

		if err := addNamed(structCols, rv, true); err != nil {
			return nil, err
		}

		if idx == 0 {
			for tagName := range structCols {
				names = append(names, tagName)
			}
		} else if len(names) != len(structCols) {
			return nil, fmt.Errorf("Length Mismatch on types")
		}

		values := make([]interface{}, 0)

		for _, tagName := range names {
			values = append(values, structCols[tagName])
		}

		builder = builder.Values(values...)
	}

	builder = builder.Columns(names...)
	return builder, nil

}
