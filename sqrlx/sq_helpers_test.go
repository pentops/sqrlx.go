package sqrlx

import "testing"

func compareSQL(t testing.TB, stmt Sqlizer, wantText string, wantArgs ...interface{}) {

	gotText, gotArgs, err := stmt.ToSql()
	if err != nil {
		t.Fatal(err.Error())
	}

	if gotText != wantText {
		t.Errorf("Want != Got: \n  %s\n  %s", wantText, gotText)
	}

	if len(wantArgs) != len(gotArgs) {
		t.Errorf("Want %d args, got %d", len(wantArgs), len(gotArgs))
	}

	for idx, want := range wantArgs {
		if want != gotArgs[idx] {
			t.Errorf("at index %d, want %v got %v", idx, want, gotArgs[idx])
		}
	}

}

func TestUpsertSimple(t *testing.T) {

	b := Upsert("table").Key("id", 1234).Set("data", "ASDF")

	compareSQL(t, b, "INSERT INTO table (id,data) VALUES (?,?) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
		1234, "ASDF")

}

func TestUpsertComplex(t *testing.T) {

	b := Upsert("table").
		Key("id", 1234).
		Key("subkey", "a").
		Set("data", "ASDF").
		Set("fieldb", true).
		Where("updated > ?", 55)

	compareSQL(t, b, "INSERT INTO table (id,subkey,data,fieldb) "+
		"VALUES (?,?,?,?) "+
		"ON CONFLICT (id,subkey) DO UPDATE SET data = EXCLUDED.data, fieldb = EXCLUDED.fieldb "+
		"WHERE updated > ?", 1234, "a", "ASDF", true, 55)

}
