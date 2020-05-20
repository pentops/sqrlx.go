package sqrlx

import (
	"fmt"
	"reflect"

	sq "github.com/elgris/sqrl"
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

		if err := addNamed(&walkBaton{
			structCols: structCols,
		}, rv); err != nil {
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

func UpdateStruct(table string, src interface{}) (*sq.UpdateBuilder, error) {

	builder := sq.Update(table)

	rv := reflect.ValueOf(src)
	if rv.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("UpdateStruct requires a pointer to a struct")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("UpdateStruct requires a pointer to a struct")
	}

	structCols := map[string]interface{}{}

	if err := addNamed(&walkBaton{
		structCols: structCols,
		override:   true,
	}, rv); err != nil {
		return nil, err
	}

	for tagName, value := range structCols {
		builder = builder.Set(tagName, value)
	}
	return builder, nil
}
