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

		values := make([]interface{}, 0)

		// TODO: Check types to raise errors
		rt := reflect.TypeOf(src).Elem()
		for i := 0; i < rv.NumField(); i++ {
			tag := rt.Field(i).Tag
			tagName := tag.Get("sql")
			if tagName == "" || tagName == "-" {
				continue
			}
			fieldInterface := rv.Field(i).Interface()
			values = append(values, fieldInterface)
			if idx == 0 {
				names = append(names, tagName)
			}
		}

		builder = builder.Values(values...)
	}

	builder = builder.Columns(names...)
	return builder, nil

}
