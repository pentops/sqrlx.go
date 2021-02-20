package sqrlx

import "fmt"

type CaseSumBuilder struct {
	Target    string
	Condition string
	Args      []interface{}
}

func (cs CaseSumBuilder) ToSql() (string, []interface{}, error) {
	return fmt.Sprintf(`SUM(CASE WHEN %s THEN COALESCE(%s,0) ELSE 0 END)`,
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
