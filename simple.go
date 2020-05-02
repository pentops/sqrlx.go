package sqrlx

import (
	"fmt"
	sq "github.com/elgris/sqrl"
	"reflect"
)

func InsertStruct(table string, src interface{}) (*sq.InsertBuilder, error) {

	builder := sq.Insert(table)

	rv := reflect.ValueOf(src)
	if rv.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("ScanStruct requires a pointer to a struct")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("ScanStruct requires a pointer to a struct")
	}

	values := make([]interface{}, 0)
	names := make([]string, 0)

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
		names = append(names, tagName)
	}

	return builder.Columns(names...).Values(values...), nil

}
