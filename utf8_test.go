package sqlanywhere

import (
	"database/sql"
	"fmt"
	"math"
	"testing"
)

func testCharSet(testdb *sql.DB, t *testing.T) {
	var err error

	tableName := fmt.Sprintf("table_%d", randInt(math.MaxInt64))
	_, err = testdb.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			row_number				int primary key,
			a_varchar_256_bytes 	varchar(256),
			a_varchar_64_runes   	varchar(64 character),
			a_varchar_long 			long varchar,
			a_nvarchar_64_runes   	nvarchar(64),
			a_nvarchar_long     	long nvarchar,
		);
	`, tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = testdb.Exec("drop table " + tableName)
		if err != nil {
			t.Fatal(err)
		}
	}()

	var a, index int
	var b, c, d, e, f string
	cases := []string{
		"Hello! 日本問付回名人亀価部肝月小男抑知。",
		"И вдаль глядел Пред ним широко",
		"ვეპხის ტყაოსანი შოთა რუსთაველი",
		"나는 유리를 먹을 수 있어요. 그래도 아프지 않아요",
		"Mogę jeść szkło i mi nie szkodzi",
		"私はガラスを食べられます。それは私を傷つけません。",
		"ฉันกินกระจกได้ แต่มันไม่ทำให้ฉั",
		"جام ييه بلورم بڭا ضررى طوقونمز",
		" ཤེལ་སྒོ་ཟ་ནས་ང་ན་གི་མ་རེད།",
		"Příliš žluťoučký kůň úpěl ďábelské kódy.",
		"Sævör grét áðan því úlpan var ónýt.",
	}
	for i, want := range cases {
		index = i + 1

		sql := fmt.Sprintf("insert into %s values (?,?,?,?,?,?)", tableName)
		_, err = testdb.Exec(sql, index, want, want, want, want, want)
		if err != nil {
			t.Fatal(err)
		}

		row := testdb.QueryRow("select * from "+tableName+" where row_number = ?", index)

		if err := row.Scan(&a, &b, &c, &d, &e, &f); err != nil {
			t.Fatal(err)
		}
		if a != index {
			t.Fatalf("want index %q, got index %q", index, a)
		}
		for colIndex, got := range []string{b, c, d, e, f} {
			if got != want {
				t.Errorf("colIndex %d: want %q, got %q", colIndex, want, got)
			}
		}
	}
}
