package sqlanywhere

import (
	"database/sql"
	"testing"
)

func testLastInsertID(db *sql.DB, t *testing.T) {

	var tx *sql.Tx
	var err error
	if tx, err = db.Begin(); err != nil {
		t.Fatal(err)
	}

	if _, err = tx.Exec(`
	create table person(
		person_id unsigned int primary key default autoincrement, 
		person_name varchar(100)
	)
	`); err != nil {
		t.Fatalf("did not create person table: %v", err)
	}

	var result sql.Result
	var got int64

	for _, want := range []int64{1, 2, 3} {

		if result, err = tx.Exec(`insert into person (person_name) values('Test')`); err != nil {
			t.Fatal(err)
		}

		if got, err = result.LastInsertId(); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("want LastInsertId %d, got %d\n", want, got)
		}

		var n int64
		if n, err = result.RowsAffected(); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected 1 row affected")
		}
	}

	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
