package sqlanywhere

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"
)

func createTable(t *testing.T, tx *sql.Tx) {
	var err error
	_, err = tx.Exec(`create table virtue(
		virtue_id int primary key default autoincrement,
		virtue_name varchar(100)
	)`)
	if err != nil {
		t.Fatalf("did not create table: %v", err)
	}

	_, err = tx.Exec(`insert into virtue (virtue_name) values('kind'),('generous'),('considerate')`)
	if err != nil {
		t.Fatal(err)
	}
}

func createProcedure(t *testing.T, tx *sql.Tx) {
	var err error
	_, err = tx.Exec(`
	CREATE PROCEDURE virtues()
	RESULT (virtue_id int, virtue_name varchar(100))
	BEGIN
		SELECT virtue_id, virtue_name from virtue order by virtue_id ASC;
		SELECT virtue_id, virtue_name from virtue order by virtue_id DESC;
	END;
	`)
	if err != nil {
		t.Fatalf("did not create procedure: %v", err)
	}
}

type virtue struct {
	id   int
	name string
}

func TestMultipleResultSet(t *testing.T) {
	testdb := NewTestDB(t)
	defer testdb.Cleanup()

	db, close := testdb.Open()
	defer close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("did not begin transaction %v", err)
	}

	createTable(t, tx)
	createProcedure(t, tx)

	// Pass a context with a timeout to tell a blocking function that it
	// should abandon its work after the timeout elapses.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	rows, err := tx.QueryContext(ctx, "CALL virtues()")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	defer rows.Close()

	resultset := [][]virtue{}

	for {
		virtues := []virtue{}
		for rows.Next() {
			virtue := virtue{}
			err = rows.Scan(&virtue.id, &virtue.name)
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			virtues = append(virtues, virtue)
		}
		resultset = append(resultset, virtues)
		if !rows.NextResultSet() {
			break
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("Err: %v", err)
	}

	//we want two result sets
	want := [][]virtue{
		{
			{1, "kind"},
			{2, "generous"},
			{3, "considerate"},
		},
		{
			{3, "considerate"},
			{2, "generous"},
			{1, "kind"},
		},
	}

	if !reflect.DeepEqual(want, resultset) {
		t.Fatalf("expected multiple result sets\nwant: %v\n got: %v\n", want, resultset)
	}

	//end the transaction
	if err = tx.Commit(); err != nil {
		t.Fatalf("did not commit: %v", err)
	}

}
