package sqrlx

import (
	"fmt"
	"strings"

	"github.com/elgris/sqrl"
)

type CaseSumBuilder struct {
	Target    string
	Condition string
	Args      []interface{}
}

func (cs CaseSumBuilder) ToSql() (string, []interface{}, error) {
	return fmt.Sprintf(`COALESCE(SUM(CASE WHEN %s THEN COALESCE(%s,0) ELSE 0 END), 0)`,
		cs.Condition,
		cs.Target,
	), cs.Args, nil
}

func CaseSum(target, condition string, args ...interface{}) *CaseSumBuilder {
	return &CaseSumBuilder{
		Target:    target,
		Condition: condition,
		Args:      args,
	}
}

type fieldPair struct {
	column string
	value  interface{}
}

type UpsertBuilder struct {
	into string
	keys []fieldPair
	vals []fieldPair

	updateStatement *sqrl.UpdateBuilder
}

func (b UpsertBuilder) ToSql() (sqlStr string, args []interface{}, err error) {

	if len(b.into) == 0 {
		err = fmt.Errorf("upsert statements must specify a table")
		return
	}
	if len(b.keys) == 0 {
		err = fmt.Errorf("upsert statements must have at least one key")
		return
	}
	if len(b.vals) == 0 {
		err = fmt.Errorf("upsert statements must have at least one value")
		return
	}

	keyList := make([]string, 0, len(b.keys))
	valList := make([]string, 0, len(b.vals))

	columns := make([]string, 0, len(b.keys)+len(b.vals))
	values := make([]interface{}, 0, len(columns))
	setMap := map[string]struct{}{}

	updateStatement := b.updateStatement

	for _, key := range b.keys {
		if _, ok := setMap[key.column]; ok {
			err = fmt.Errorf("duplicate column in keys and values: %s", key.column)
		}
		setMap[key.column] = struct{}{}
		columns = append(columns, key.column)
		values = append(values, key.value)
		keyList = append(keyList, key.column)
	}

	for _, set := range b.vals {
		if _, ok := setMap[set.column]; ok {
			err = fmt.Errorf("duplicate column in keys and values: %s", set.column)
		}
		setMap[set.column] = struct{}{}
		columns = append(columns, set.column)
		values = append(values, set.value)
		valList = append(valList, fmt.Sprintf("%s = EXCLUDED.%s", set.column, set.column))
		updateStatement.Set(set.column, sqrl.Expr(fmt.Sprintf("EXCLUDED.%s", set.column)))
	}

	//	suffix := fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s", strings.Join(keyList, ","), strings.Join(valList, ", "))
	updateStatement.ToSql()
	updateString, suffixArgs, err := updateStatement.ToSql()
	if err != nil {
		return
	}

	if updateString[0:9] != "UPDATE _ " {
		err = fmt.Errorf("unexpected update string: %s", updateString[0:9])
		return
	}

	updateString = fmt.Sprintf("ON CONFLICT (%s) DO UPDATE %s", strings.Join(keyList, ","), updateString[9:])

	return sqrl.Insert(b.into).Columns(columns...).Values(values...).Suffix(updateString, suffixArgs...).ToSql()

}

func Upsert(into string) *UpsertBuilder {
	return &UpsertBuilder{
		into:            into,
		updateStatement: sqrl.Update("_"),
	}
}

func (u *UpsertBuilder) Key(column string, value interface{}) *UpsertBuilder {
	u.keys = append(u.keys, fieldPair{
		column: column,
		value:  value,
	})
	return u
}

func (u *UpsertBuilder) Set(column string, value interface{}) *UpsertBuilder {
	u.vals = append(u.vals, fieldPair{
		column: column,
		value:  value,
	})
	return u
}

func (u *UpsertBuilder) SetMap(vals map[string]interface{}) *UpsertBuilder {
	for k, v := range vals {
		u.Set(k, v)
	}
	return u
}

func (u *UpsertBuilder) Where(pred interface{}, args ...interface{}) *UpsertBuilder {
	u.updateStatement.Where(pred, args...)
	return u
}
